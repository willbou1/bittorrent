use std::env;

fn decode_bencoded_value(encoded_value: &str) -> (Optional<serde_json::Value>, &str) {
    let first_char = encoded_value.chars().next().unwrap();
    match first_char {
        'e' => (None, &encoded_value[1..]),
        'l' => {
            let mut elements = vec![];
            while let (Some(element), remaining) = decode_bencoded_value(encoded_value) {
                elements.push(element);
            }
            (Some(serde_json::Value::Array(elements)), remaining)
        },
        'i' => {
            let e_index = encoded_value .find('e')
                .unwrap_or_else(|| panic!("No e in bencoded integer: {encoded_value}"));
            let integer_string = &encoded_value[1..e_index];
            let integer = integer_string.parse::<i128>()
                .unwrap_or_else(|_| panic!("Invalid number in bencoded integer: {integer_string}"));
            let number = serde_json::Number::from_i128(integer).unwrap();
            (Some(serde_json::Value::Number(number)), &encoded_value[(e_index + 1)..])
        },
        _ if first_char.is_ascii_digit() => {
            let colon_index = encoded_value .find(':')
                .unwrap_or_else(|| panic!("No colon in bencoded string: {encoded_value}"));
            let number_string = &encoded_value[..colon_index];
            let number = number_string.parse::<usize>()
                .unwrap_or_else(|_| panic!("Invalid length in bencoded string: {number_string}"));
            let string = &encoded_value[colon_index + 1..colon_index + 1 + number];
            (Some(serde_json::Value::String(string.to_string())), &encoded_value[(colon_index + number + 1)..])
        },
        _ => {
            panic!("Unhandled bencoded value: {}", encoded_value)
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let command = &args[1];

    if command == "decode" {
        // eprintln!("Logs from your program will appear here!");

        let encoded_value = &args[2];
        let decoded_value = decode_bencoded_value(encoded_value);
        println!("{}", decoded_value.to_string());
    } else {
        println!("unknown command: {}", args[1])
    }
}
