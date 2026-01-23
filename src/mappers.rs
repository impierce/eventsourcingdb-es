pub(crate) fn map_event_type_to_reverse_domain_name(
    event_type: &str,
    event_version: &str,
) -> String {
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

    format!("org.example.{}.v{}", parts.join("."), event_version)
}

pub(crate) fn map_reverse_domain_name_to_type(reverse_domain_name: &str) -> (String, String) {
    let parts: Vec<&str> = reverse_domain_name
        .trim_start_matches("org.example.")
        .split('.')
        .collect();

    let (type_parts, version) = parts.split_at(parts.len() - 1);

    let version = version[0].trim_start_matches('v').to_string();

    let ty = type_parts
        .iter()
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<String>>()
        .join("");

    (ty, version)
}

pub(crate) fn map_aggregate_type_and_id_to_subject(
    aggregate_type: &str,
    aggregate_id: &str,
) -> String {
    format!("/{}/{}", aggregate_type.to_lowercase(), aggregate_id)
}

pub(crate) fn map_subject_to_aggregate_type_and_id(subject: &str) -> (String, String) {
    let parts: Vec<&str> = subject.trim_start_matches('/').split('/').collect();

    // Capitalize the first letter
    let mut v: Vec<char> = parts[0].to_string().chars().collect();
    v[0] = v[0].to_uppercase().nth(0).unwrap();
    let aggregate_type = v.into_iter().collect();

    let aggregate_id = parts[1].to_string();
    (aggregate_type, aggregate_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_type_to_reverse_domain_name() {
        let event_type = "UserCreated";
        let expected = "org.example.user.created.v1";
        let result = map_event_type_to_reverse_domain_name(event_type, "1");
        assert_eq!(result, expected);
    }

    #[test]
    fn test_map_reverse_domain_name_to_type() {
        let reverse_domain_name = "org.example.user.created.v1";
        let expected_type = "UserCreated";
        let expected_version = "1".to_string();
        let result = map_reverse_domain_name_to_type(reverse_domain_name);
        assert_eq!(result, (expected_type.to_string(), expected_version));
    }

    #[test]
    fn test_map_aggregate_type_to_subject() {
        let aggregate_type = "Customer";
        let aggregate_id = "12345";
        let expected = "/customer/12345";
        let result = map_aggregate_type_and_id_to_subject(aggregate_type, aggregate_id);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_map_subject_to_aggregate_type_and_id() {
        let subject = "/customer/12345";
        let (aggregate_type, aggregate_id) = map_subject_to_aggregate_type_and_id(subject);
        assert_eq!(aggregate_type, "Customer");
        assert_eq!(aggregate_id, "12345");
    }
}
