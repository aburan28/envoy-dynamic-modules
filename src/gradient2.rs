/// Parameters for the Gradient2 concurrency limit calculation.
pub struct Gradient2Params {
    pub current_limit: u32,
    pub min_rtt_ns: u64,
    pub sample_rtt_ns: u64,
    pub min_limit: u32,
    pub max_limit: u32,
    pub max_gradient: f64,
    pub min_rtt_buffer_pct: f64,
}

/// Result of a Gradient2 calculation, including the intermediate gradient value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Gradient2Result {
    pub new_limit: u32,
    pub gradient: f64,
}

/// Calculate a new concurrency limit using the Gradient2 algorithm.
///
/// The algorithm computes:
///   buffered_min_rtt = min_rtt * (1 + buffer_pct)
///   gradient = clamp(buffered_min_rtt / sample_rtt, 0.5, max_gradient)
///   headroom = sqrt(current_limit)
///   new_limit = clamp(current_limit * gradient + headroom, min, max)
pub fn calculate_new_limit(params: &Gradient2Params) -> Gradient2Result {
    if params.min_rtt_ns == 0 || params.sample_rtt_ns == 0 {
        return Gradient2Result {
            new_limit: params.current_limit,
            gradient: 1.0,
        };
    }

    let buffered_min_rtt = params.min_rtt_ns as f64 * (1.0 + params.min_rtt_buffer_pct);
    let gradient = (buffered_min_rtt / params.sample_rtt_ns as f64).clamp(0.5, params.max_gradient);
    let headroom = (params.current_limit as f64).sqrt();
    let raw = params.current_limit as f64 * gradient + headroom;
    let new_limit = (raw.round() as u32).clamp(params.min_limit, params.max_limit);

    Gradient2Result {
        new_limit,
        gradient,
    }
}

/// Compute the value at a given percentile from a sorted slice of samples.
/// Uses the nearest-rank method. `p` should be in [0.0, 1.0].
pub fn percentile(sorted_samples: &[u64], p: f64) -> u64 {
    if sorted_samples.is_empty() {
        return 0;
    }
    if sorted_samples.len() == 1 {
        return sorted_samples[0];
    }
    let index = ((p * sorted_samples.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted_samples.len() - 1);
    sorted_samples[index]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(current: u32, min_rtt: u64, sample_rtt: u64) -> Gradient2Params {
        Gradient2Params {
            current_limit: current,
            min_rtt_ns: min_rtt,
            sample_rtt_ns: sample_rtt,
            min_limit: 3,
            max_limit: 1000,
            max_gradient: 2.0,
            min_rtt_buffer_pct: 0.25,
        }
    }

    #[test]
    fn steady_state_grows_by_headroom() {
        let result = calculate_new_limit(&params(100, 5_000_000, 6_250_000));
        assert!((result.gradient - 1.0).abs() < 0.001);
        assert_eq!(result.new_limit, 110);
    }

    #[test]
    fn latency_increase_reduces_limit() {
        let result = calculate_new_limit(&params(100, 5_000_000, 12_500_000));
        assert!((result.gradient - 0.5).abs() < 0.001);
        assert_eq!(result.new_limit, 60);
    }

    #[test]
    fn gradient_floor_at_half() {
        let result = calculate_new_limit(&params(100, 5_000_000, 100_000_000));
        assert!((result.gradient - 0.5).abs() < 0.001);
    }

    #[test]
    fn gradient_capped_at_max() {
        let result = calculate_new_limit(&params(100, 5_000_000, 1_000_000));
        assert!((result.gradient - 2.0).abs() < 0.001);
        assert_eq!(result.new_limit, 210);
    }

    #[test]
    fn percentile_returns_expected_rank() {
        let samples: Vec<u64> = (1..=10).collect();
        assert_eq!(percentile(&samples, 0.9), 9);
        assert_eq!(percentile(&samples, 0.5), 5);
    }

    #[test]
    fn convergence_under_sustained_latency() {
        let mut limit = 100u32;
        for _ in 0..20 {
            let result = calculate_new_limit(&Gradient2Params {
                current_limit: limit,
                min_rtt_ns: 5_000_000,
                sample_rtt_ns: 20_000_000,
                min_limit: 3,
                max_limit: 1000,
                max_gradient: 2.0,
                min_rtt_buffer_pct: 0.25,
            });
            limit = result.new_limit;
        }
        assert!(limit < 10, "limit should converge low, got {limit}");
    }
}
