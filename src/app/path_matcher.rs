use std::collections::hash_map::Values;
use std::collections::HashMap;

pub type PathParams = HashMap<String, String>;

/// Lookup mechanism that uses inexact matching of input paths by finding the most specific
/// match and returning that instead.  See [`PathMatcher::lookup`] for more information.
#[derive(Debug)]
pub struct PathMatcher<V> {
    map: HashMap<Vec<String>, V>,
}

impl<V> FromIterator<(Vec<String>, V)> for PathMatcher<V> {
    fn from_iter<I: IntoIterator<Item = (Vec<String>, V)>>(iter: I) -> Self {
        Self {
            map: HashMap::from_iter(iter),
        }
    }
}

impl<V> PathMatcher<V> {
    pub fn new_empty() -> Self {
        Self {
            map: HashMap::default(),
        }
    }

    /// Convert canonical path strings into the more efficient PathMatcher variation.  Input paths
    /// are expected to be in the form of "/foo/bar".
    pub fn from_path_strings(src: HashMap<String, V>) -> Self {
        let iter = src.into_iter().map(|(k, v)| (key_from_path(&k), v));
        Self::from_iter(iter)
    }

    /// Finds the most specific matched result based on the provided input path.  Does not
    /// require an exact match of input path!  See [`MatchedResult`] for more information.
    pub fn lookup(&self, path: &[String]) -> Option<MatchedResult<V>> {
        for search_depth in (0..path.len() + 1).rev() {
            let mut best = None;
            for (pattern, value) in &self.map {
                if pattern.len() != search_depth {
                    continue;
                }
                let Some(params) = match_path(pattern, &path[..search_depth]) else {
                    continue;
                };
                let literal_count = pattern
                    .iter()
                    .filter(|segment| !is_parameter(segment))
                    .count();
                if best
                    .as_ref()
                    .is_none_or(|(best_count, _): &(usize, _)| literal_count > *best_count)
                {
                    best = Some((literal_count, (value, params)));
                }
            }
            if let Some((_, (value, params))) = best {
                return Some(MatchedResult {
                    value,
                    matched_index: search_depth,
                    params,
                });
            }
        }
        None
    }

    /// Identical to `lookup` except that it doesn't stop once the most specific match has been
    /// found, but rather returns all potential matches.  For example, if the input path is
    /// "/foo/bar", and the matcher contains "/foo", "/foo/bar", "/baz", the result will be
    /// matched results for "/foo" and "/foo/bar".
    pub fn match_all(&self, path: &[String]) -> Vec<MatchedResult<V>> {
        let mut result = Vec::new();
        for search_depth in (0..path.len() + 1).rev() {
            for (pattern, value) in &self.map {
                if pattern.len() != search_depth {
                    continue;
                }
                if let Some(params) = match_path(pattern, &path[..search_depth]) {
                    result.push(MatchedResult {
                        value,
                        matched_index: search_depth,
                        params,
                    });
                }
            }
        }
        result
    }

    pub fn insert(&mut self, key: Vec<String>, value: V) -> Option<V> {
        self.map.insert(key, value)
    }

    pub fn remove(&mut self, key: &[String]) -> Option<V> {
        self.map.remove(key)
    }

    pub fn values(&self) -> Values<'_, Vec<String>, V> {
        self.map.values()
    }

    pub fn lookup_exact(&self, path: &[String]) -> Option<&V> {
        self.map.get(path)
    }
}

pub fn key_from_path(path: &str) -> Vec<String> {
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// Looked up value but also the index at which the match occurred so that the caller can determine
/// how much of the input path was unmatched and considered "dangling".  For example, the input
/// path could be `["foo","bar"]` but the match was for `["foo"]`.  The `matched_index` would be 1
/// such that `&input[matched_index..]` would yield `["bar"]`.
pub struct MatchedResult<'a, V> {
    pub value: &'a V,
    pub matched_index: usize,
    pub params: PathParams,
}

fn is_parameter(segment: &str) -> bool {
    segment.starts_with('{') && segment.ends_with('}') && segment.len() > 2
}

fn match_path(pattern: &[String], path: &[String]) -> Option<PathParams> {
    let mut params = PathParams::new();
    for (pattern_segment, path_segment) in pattern.iter().zip(path) {
        if is_parameter(pattern_segment) {
            let name = &pattern_segment[1..pattern_segment.len() - 1];
            if params
                .insert(name.to_owned(), path_segment.clone())
                .is_some()
            {
                return None;
            }
        } else if pattern_segment != path_segment {
            return None;
        }
    }
    Some(params)
}

#[cfg(test)]
mod tests {
    use crate::app::path_matcher::PathMatcher;

    #[test]
    fn test_unambiguous_exact_match() {
        let matcher = new_matcher(["/a/b/c", "/a/b/d"]);
        assert_exact_match(&matcher, "/a/b/c");
        assert_exact_match(&matcher, "/a/b/d");
    }

    #[test]
    fn test_ambiguous_match() {
        let matcher = new_matcher(["/a/b/c", "/a/b/d"]);
        assert_no_match(&matcher, "/a/b");
    }

    #[test]
    fn test_inexact_match() {
        let matcher = new_matcher(["/a/b/c"]);
        assert_prefix_match(&matcher, "/a/b/c/d", "/a/b/c", "d");
    }

    #[test]
    fn test_dynamic_match() {
        let matcher = new_matcher(["/api/v1/{token}/telemetry"]);
        let result = matcher
            .lookup(&path_to_vec("/api/v1/device-token/telemetry"))
            .unwrap();
        assert_eq!(result.params.get("token"), Some(&"device-token".to_owned()));
    }

    #[test]
    fn test_literal_match_beats_dynamic_match() {
        let matcher = new_matcher(["/api/{value}/status", "/api/v1/status"]);
        let result = matcher.lookup(&path_to_vec("/api/v1/status")).unwrap();
        assert_eq!(result.value, "/api/v1/status");
        assert!(result.params.is_empty());
    }

    fn new_matcher<const N: usize>(paths: [&str; N]) -> PathMatcher<String> {
        let path_kv_pairs = paths.into_iter().map(|path| {
            let key = path_to_vec(path);
            let value = path.to_string();
            (key, value)
        });

        PathMatcher::from_iter(path_kv_pairs)
    }

    fn assert_no_match(matcher: &PathMatcher<String>, path: &str) {
        let paths = path_to_vec(path);
        assert!(matcher.lookup(&paths).is_none());
    }

    fn assert_exact_match(matcher: &PathMatcher<String>, path: &str) {
        assert_prefix_match(matcher, path, path, "");
    }

    fn assert_prefix_match(
        matcher: &PathMatcher<String>,
        path: &str,
        expected_value: &str,
        expected_suffix: &str,
    ) {
        let paths = path_to_vec(path);

        let result = matcher.lookup(&paths);
        let matched = result.unwrap_or_else(|| panic!("Expected {expected_value}"));
        assert_eq!(matched.value, expected_value);

        let actual_suffix = &paths[matched.matched_index..];
        assert_eq!(actual_suffix, &path_to_vec(expected_suffix));
    }

    fn path_to_vec(path: &str) -> Vec<String> {
        if path.is_empty() {
            return vec![];
        }
        path.split('/').map(|p| p.to_string()).collect()
    }
}
