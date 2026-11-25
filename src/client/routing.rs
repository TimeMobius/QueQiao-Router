use crate::client::cache_weave;
use crate::config::types::{ClientConfig, LoadBalancingStrategy};
use rand::prelude::*;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// 轮次几何衰减系数：同一会话中每往回一轮，该轮锚点的票权乘以该值。
///
/// 实测长会话（94 轮 user 消息）在纯长度加权下需要前 41 轮才占一半票权，
/// 首轮或早期长消息会把会话永久锁死在同一后端；取 0.85（≈4.3 轮半衰期）
/// 后“最近约 5 轮占一半”，既让近轮次有话语权，又把换后端比例控制在很低水平。
const TURN_DECAY: f64 = 0.85;

/// 锚点票权：基础长度 × `TURN_DECAY^turns_after`。
/// 最新一轮（`turns_after = 0`）权重等于其字节长度，越旧按几何级数衰减。
fn anchor_weight(length_bytes: usize, turns_after: usize) -> f64 {
    length_bytes as f64 * TURN_DECAY.powi(turns_after as i32)
}

/// 选择客户端的策略函数（兼容没有 model 的旧调用）。
pub fn select_clients(
    clients: Vec<ClientConfig>,
    routing_keys: Option<Vec<(String, usize, usize)>>,
    strategy: &LoadBalancingStrategy,
    active_counts: Option<&HashMap<String, i64>>,
) -> Vec<ClientConfig> {
    select_clients_with_model(clients, routing_keys, strategy, active_counts, "")
}

/// 带模型名的策略入口：CacheWeave 需要 model 读取按 `model:backend` 聚合的
/// TTFT/TPS 窗口；其他策略忽略该参数，行为不变。
pub fn select_clients_with_model(
    clients: Vec<ClientConfig>,
    routing_keys: Option<Vec<(String, usize, usize)>>,
    strategy: &LoadBalancingStrategy,
    active_counts: Option<&HashMap<String, i64>>,
    model: &str,
) -> Vec<ClientConfig> {
    if clients.is_empty() {
        return clients;
    }

    if strategy.is_cache_weave() {
        let order = cache_weave::order(&clients, active_counts, model, routing_keys.as_deref());
        let mut slots: Vec<Option<ClientConfig>> = clients.into_iter().map(Some).collect();
        let mut ordered = Vec::with_capacity(slots.len());
        for idx in order {
            if let Some(client) = slots.get_mut(idx).and_then(Option::take) {
                ordered.push(client);
            }
        }
        ordered.extend(slots.into_iter().flatten());
        return ordered;
    }

    match strategy {
        LoadBalancingStrategy::Random => select_clients_by_random_weight(clients),
        LoadBalancingStrategy::Deterministic => {
            if let Some(keys) = routing_keys {
                if !keys.is_empty() {
                    return select_clients_by_voting(clients, keys);
                }
            }
            select_clients_by_random_weight(clients)
        }
        LoadBalancingStrategy::LeastConnections => {
            select_clients_by_least_connections(clients, active_counts)
        }
        LoadBalancingStrategy::CacheWeave => {
            select_clients_by_least_connections(clients, active_counts)
        }
    }
}

/// 传统的加权随机算法 (Weighted Random Sampling)
fn select_clients_by_random_weight(clients: Vec<ClientConfig>) -> Vec<ClientConfig> {
    let mut rng = rand::thread_rng();
    let mut weighted_clients: Vec<(f64, ClientConfig)> = clients
        .into_iter()
        .map(|client| {
            let weight = client.priority.unwrap_or(0) as f64;
            if weight <= 0.0 {
                (0.0, client)
            } else {
                let random_value: f64 = rng.gen();
                let sort_key = random_value.powf(1.0 / weight);
                (sort_key, client)
            }
        })
        .collect();

    weighted_clients.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    weighted_clients
        .into_iter()
        .map(|(_, client)| client)
        .collect()
}

/// 基于多锚点加权投票的确定性路由算法
fn select_clients_by_voting(
    clients: Vec<ClientConfig>,
    keys: Vec<(String, usize, usize)>,
) -> Vec<ClientConfig> {
    let mut client_scores: HashMap<String, f64> = HashMap::new();

    for (key_content, length_bytes, turns_after) in keys {
        let mut best_client_name = String::new();
        let mut max_hash_score = -1.0;

        for client in &clients {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key_content.hash(&mut hasher);
            client.name.hash(&mut hasher);
            let hash_val = hasher.finish();

            let normalized_hash = ((hash_val as f64) + 1.0) / ((u64::MAX as f64) + 1.0);
            let priority = client.priority.unwrap_or(0) as f64;

            let score = if priority <= 0.0 {
                0.0
            } else {
                normalized_hash.powf(1.0 / priority)
            };

            if score > max_hash_score {
                max_hash_score = score;
                best_client_name = client.name.clone();
            }
        }

        if !best_client_name.is_empty() {
            let weight = anchor_weight(length_bytes, turns_after);
            let entry = client_scores.entry(best_client_name).or_insert(0.0);
            *entry += weight;
        }
    }

    let mut ranked_clients: Vec<(f64, ClientConfig)> = clients
        .into_iter()
        .map(|c| {
            let score = *client_scores.get(&c.name).unwrap_or(&0.0);
            (score, c)
        })
        .collect();

    ranked_clients.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.name.cmp(&b.1.name))
    });

    ranked_clients.into_iter().map(|(_, c)| c).collect()
}

/// 加权最少连接路由算法
fn select_clients_by_least_connections(
    clients: Vec<ClientConfig>,
    active_counts: Option<&HashMap<String, i64>>,
) -> Vec<ClientConfig> {
    select_clients_by_least_connections_with_rng(clients, active_counts, &mut rand::thread_rng())
}

fn select_clients_by_least_connections_with_rng(
    clients: Vec<ClientConfig>,
    active_counts: Option<&HashMap<String, i64>>,
    rng: &mut impl Rng,
) -> Vec<ClientConfig> {
    let empty_map = HashMap::new();
    let counts = active_counts.unwrap_or(&empty_map);

    let mut scored_clients: Vec<(f64, ClientConfig)> = clients
        .into_iter()
        .map(|client| {
            let active = *counts.get(&client.name).unwrap_or(&0);
            let priority = client.priority.unwrap_or(1).max(1) as f64;
            let score = if active <= 0 {
                0.0_f64
            } else {
                active as f64 / priority
            };
            (score, client)
        })
        .collect();

    // 先打散，再按负载稳定排序：同负载（尤其全部空闲）时不固定偏向某个名字。
    scored_clients.shuffle(rng);
    scored_clients.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    scored_clients.into_iter().map(|(_, c)| c).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{ClientConfig, ModelMatch};
    use std::hash::{Hash, Hasher};

    /// 复刻生产代码的 rendezvous hash，用于构造“新近轮次换后端”的确定性用例。
    fn winner_for_key(key: &str, clients: &[ClientConfig]) -> String {
        let mut best_name = String::new();
        let mut best_score = -1.0_f64;
        for client in clients {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            key.hash(&mut hasher);
            client.name.hash(&mut hasher);
            let normalized_hash = ((hasher.finish() as f64) + 1.0) / ((u64::MAX as f64) + 1.0);
            let priority = client.priority.unwrap_or(0) as f64;
            let score = if priority <= 0.0 {
                0.0
            } else {
                normalized_hash.powf(1.0 / priority)
            };
            if score > best_score {
                best_score = score;
                best_name = client.name.clone();
            }
        }
        best_name
    }

    fn create_test_client(name: &str, priority: u32) -> ClientConfig {
        ClientConfig {
            name: name.to_string(),
            base_url: "http://example.com".to_string(),
            api_key: None,
            model_match: ModelMatch {
                match_type: "exact".to_string(),
                value: vec!["test-model".to_string()],
            },
            priority: Some(priority),
            fallback: None,
            special_prefix: None,
            stop: None,
            max_tokens: None,
            extra_body: None,
            ..Default::default()
        }
    }

    #[test]
    fn test_select_clients_by_random_weight() {
        let clients = vec![
            create_test_client("client1", 1),
            create_test_client("client2", 3),
            create_test_client("client3", 2),
        ];

        let selected = select_clients_by_random_weight(clients);
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn test_select_clients_by_random_weight_empty() {
        let clients: Vec<ClientConfig> = vec![];
        let selected = select_clients_by_random_weight(clients);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_clients_with_routing_keys() {
        let clients = vec![
            create_test_client("client1", 10),
            create_test_client("client2", 10),
        ];
        let strategy = LoadBalancingStrategy::Deterministic;
        let routing_keys = vec![("test content here".to_string(), 100, 0)];
        let selected = select_clients(clients.clone(), Some(routing_keys), &strategy, None);
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_select_clients_empty_input() {
        let clients: Vec<ClientConfig> = vec![];
        let selected = select_clients(clients, None, &LoadBalancingStrategy::Deterministic, None);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_clients_single_client() {
        let clients = vec![create_test_client("client1", 5)];
        let selected = select_clients(clients, None, &LoadBalancingStrategy::Deterministic, None);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].name, "client1");
    }

    #[test]
    fn test_deterministic_routing_same_input() {
        let clients = vec![
            create_test_client("server1", 10),
            create_test_client("server2", 10),
        ];
        let routing_keys = vec![("hello world".to_string(), 50, 0)];

        let result1 = select_clients_by_voting(clients.clone(), routing_keys.clone());
        let result2 = select_clients_by_voting(clients, routing_keys);

        assert_eq!(result1[0].name, result2[0].name);
    }

    #[test]
    fn test_deterministic_routing_different_content() {
        let clients = vec![
            create_test_client("server1", 10),
            create_test_client("server2", 10),
        ];
        let keys1 = vec![("content A".to_string(), 50, 0)];
        let keys2 = vec![("content B".to_string(), 50, 0)];

        let result1 = select_clients_by_voting(clients.clone(), keys1);
        let result2 = select_clients_by_voting(clients, keys2);

        assert!(
            result1[0].name != result2[0].name
                || result1[0].name == "server1" && result2[0].name == "server1"
        );
    }

    #[test]
    fn test_anchor_weight_geometric_turn_decay() {
        // 最新一轮不打折，每往回一轮 ×0.85
        assert!((anchor_weight(100, 0) - 100.0).abs() < 1e-9);
        assert!((anchor_weight(100, 1) - 85.0).abs() < 1e-9);
        assert!((anchor_weight(100, 2) - 72.25).abs() < 1e-9);
        assert!((anchor_weight(100, 4) - 100.0 * 0.85_f64.powi(4)).abs() < 1e-9);
    }

    #[test]
    fn test_turn_decay_lets_recent_turn_win() {
        let clients = vec![
            create_test_client("alpha", 10),
            create_test_client("beta", 10),
        ];

        // 找出分别哈希到 alpha / beta 的锚点文本，避免依赖具体哈希常数。
        let (mut old_key, mut new_key) = (None, None);
        for i in 0..256 {
            let candidate = format!("decay-probe-{i}");
            match winner_for_key(&candidate, &clients).as_str() {
                "alpha" if old_key.is_none() => old_key = Some(candidate),
                "beta" if new_key.is_none() => new_key = Some(candidate),
                _ => {}
            }
            if old_key.is_some() && new_key.is_some() {
                break;
            }
        }
        let old_key = old_key.expect("未找到命中 alpha 的锚点");
        let new_key = new_key.expect("未找到命中 beta 的锚点");

        // 旧轮 100B、新轮 90B：不打折时旧轮 100 > 90，
        // 轮次衰减后旧轮只剩 100 × 0.85 = 85 < 90，新轮反超。
        let keys = vec![(old_key, 100, 1), (new_key, 90, 0)];
        let selected = select_clients_by_voting(clients, keys);
        assert_eq!(selected[0].name, "beta");
    }

    #[test]
    fn test_turn_decay_keeps_clear_winner() {
        let clients = vec![
            create_test_client("alpha", 10),
            create_test_client("beta", 10),
        ];

        let (mut old_key, mut new_key) = (None, None);
        for i in 0..256 {
            let candidate = format!("clear-probe-{i}");
            match winner_for_key(&candidate, &clients).as_str() {
                "alpha" if old_key.is_none() => old_key = Some(candidate),
                "beta" if new_key.is_none() => new_key = Some(candidate),
                _ => {}
            }
            if old_key.is_some() && new_key.is_some() {
                break;
            }
        }
        let old_key = old_key.expect("未找到命中 alpha 的锚点");
        let new_key = new_key.expect("未找到命中 beta 的锚点");

        // 旧轮优势足够大（200 × 0.85 = 170 > 10），衰减不应翻转结果。
        let keys = vec![(old_key, 200, 1), (new_key, 10, 0)];
        let selected = select_clients_by_voting(clients, keys);
        assert_eq!(selected[0].name, "alpha");
    }

    #[test]
    fn test_least_connections_empty_counts() {
        let clients = vec![
            create_test_client("client1", 10),
            create_test_client("client2", 5),
        ];
        let selected = select_clients_by_least_connections(clients, None);
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_least_connections_with_counts() {
        let clients = vec![
            create_test_client("client1", 10),
            create_test_client("client2", 5),
            create_test_client("client3", 1),
        ];
        let mut counts = HashMap::new();
        counts.insert("client1".to_string(), 50);
        counts.insert("client2".to_string(), 5);
        counts.insert("client3".to_string(), 0);

        let selected = select_clients_by_least_connections(clients, Some(&counts));
        assert_eq!(selected.len(), 3);
        // client3 has 0 active -> score 0 -> first
        assert_eq!(selected[0].name, "client3");
        // client2 has 5/5 = 1.0 -> second
        assert_eq!(selected[1].name, "client2");
        // client1 has 50/10 = 5.0 -> third
        assert_eq!(selected[2].name, "client1");
    }

    #[test]
    fn test_least_connections_breaks_idle_ties_without_name_bias() {
        use rand::{rngs::StdRng, SeedableRng};
        let clients = vec![
            create_test_client("backend-1", 1),
            create_test_client("backend-2", 1),
            create_test_client("backend-fp8", 1),
        ];
        let mut rng = StdRng::seed_from_u64(42);
        let mut winners = std::collections::HashSet::new();
        for _ in 0..64 {
            let ranked =
                select_clients_by_least_connections_with_rng(clients.clone(), None, &mut rng);
            assert_eq!(ranked.len(), clients.len());
            winners.insert(ranked[0].name.clone());
        }
        assert_eq!(winners.len(), clients.len());
    }

    #[test]
    fn test_random_strategy_ignores_routing_keys() {
        let clients = vec![
            create_test_client("client1", 1),
            create_test_client("client2", 1),
        ];
        let keys = Some(vec![("hello".to_string(), 10, 0)]);
        let selected = select_clients(clients, keys, &LoadBalancingStrategy::Random, None);
        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_deterministic_fallback_to_random() {
        let clients = vec![
            create_test_client("client1", 1),
            create_test_client("client2", 1),
        ];
        let selected = select_clients(clients, None, &LoadBalancingStrategy::Deterministic, None);
        assert_eq!(selected.len(), 2);
    }
}
