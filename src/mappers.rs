pub(crate) fn map_type_to_reverse_domain_name(event_type: &str) -> String {
    let re = regex::Regex::new(
        r"(?x)
        (?:[A-Z][a-z]+) | # A capital letter followed by one or more lowercase letters
        (?:[A-Z]+)         # One or more capital letters
    ",
    )
    .unwrap();

    let parts: Vec<String> = re
        .find_iter(event_type)
        .map(|m| m.as_str().to_lowercase())
        .collect();

    format!("org.example.{}", parts.join("."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_type_to_reverse_domain_name() {
        let event_type = "UserCreated";
        let expected = "org.example.user.created";
        let result = map_type_to_reverse_domain_name(event_type);
        assert_eq!(result, expected);
    }
}
