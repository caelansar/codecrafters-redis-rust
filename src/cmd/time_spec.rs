use std::cmp::Ordering;
use std::str::FromStr;

#[derive(Clone, PartialEq, Debug)]
pub enum TimeSepc {
    StartWildcard,
    Partially(u64),
    Specified(u64, u32),
    EndWildcard,
}

impl FromStr for TimeSepc {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "-" {
            return Ok(TimeSepc::StartWildcard);
        }
        if s == "+" {
            return Ok(TimeSepc::EndWildcard);
        }
        if s == "$" {
            return Ok(TimeSepc::EndWildcard);
        }
        let parts = s.split_once('-');
        match parts {
            Some((ts, seq)) => {
                let ts: u64 = ts.parse()?;
                let seq: u32 = seq.parse()?;
                Ok(TimeSepc::Specified(ts, seq))
            }
            None => {
                let ts: u64 = s.parse()?;
                Ok(TimeSepc::Partially(ts))
            }
        }
    }
}

impl PartialOrd for TimeSepc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (TimeSepc::Partially(a), TimeSepc::Partially(b)) => a.partial_cmp(b),
            (TimeSepc::Partially(a), TimeSepc::Specified(b1, b2)) => {
                let result = a.partial_cmp(b1)?;
                match result {
                    Ordering::Equal => 0.partial_cmp(b2),
                    _ => Some(result),
                }
            }
            (TimeSepc::Specified(a, _), TimeSepc::Partially(b)) => a.partial_cmp(b),
            (TimeSepc::Specified(a1, b1), TimeSepc::Specified(a2, b2)) => {
                let result = a1.partial_cmp(a2)?;
                match result {
                    Ordering::Equal => b1.partial_cmp(b2),
                    _ => Some(result),
                }
            }
            (_, TimeSepc::EndWildcard) => Some(Ordering::Less),
            (TimeSepc::EndWildcard, _) => Some(Ordering::Greater),
            (TimeSepc::StartWildcard, _) => Some(Ordering::Less),
            (a, b) => {
                println!("try to compare a: {:?}, b: {:?}", a, b);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cmd::time_spec::TimeSepc;

    #[test]
    fn test_time_spec_parse() {
        let s = "2333";
        let t: TimeSepc = s.parse().unwrap();
        assert_eq!(TimeSepc::Partially(2333), t);

        let s = "111-2";
        let t: TimeSepc = s.parse().unwrap();
        assert_eq!(TimeSepc::Specified(111, 2), t);
    }

    #[test]
    fn test_time_spec_cmp() {
        assert!(TimeSepc::Specified(111, 2) <= TimeSepc::Partially(111));
        assert!(TimeSepc::Specified(111, 2) > TimeSepc::Specified(111, 1));
        assert!(!(TimeSepc::Specified(111, 2) > TimeSepc::Specified(111, 3)));
        assert!(TimeSepc::Specified(111, 2) >= TimeSepc::Partially(111));
        assert!(!(TimeSepc::Specified(111, 2) >= TimeSepc::Partially(222)));
        assert!(TimeSepc::Partially(111) < TimeSepc::Specified(111, 1));
        assert!(
            (TimeSepc::Partially(1)..TimeSepc::Partially(4)).contains(&TimeSepc::Specified(3, 100))
        )
    }
}
