//! Utility functions for debugging byte sequences

/// Format bytes as a hex string with spaces between bytes
/// Example: [0x01, 0x02, 0x03] => "01 02 03"
pub fn format_hex(data: &[u8]) -> String {
    data.iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Format bytes as a hex string with detailed information
/// Similar to hexdump output but more compact
/// Example output: "01 02 03 04 05  |.....|"
pub fn format_hex_detailed(data: &[u8], max_bytes: usize) -> String {
    let truncated = data.len() > max_bytes;
    let display_data = if truncated { &data[..max_bytes] } else { data };

    let hex_part = display_data
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ");

    let ascii_part: String = display_data
        .iter()
        .map(|&b| {
            if (32..=126).contains(&b) {
                b as char
            } else {
                '.'
            }
        })
        .collect();

    if truncated {
        format!(
            "{} ... (truncated, total {} bytes)  |{}...|",
            hex_part,
            data.len(),
            ascii_part
        )
    } else {
        format!("{}  |{}|", hex_part, ascii_part)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_hex() {
        let data = vec![0x01, 0x02, 0x03, 0x10, 0xff];
        assert_eq!(format_hex(&data), "01 02 03 10 ff");
    }

    #[test]
    fn test_format_hex_detailed() {
        let data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
        let result = format_hex_detailed(&data, 10);
        assert_eq!(result, "48 65 6c 6c 6f  |Hello|");
    }

    #[test]
    fn test_format_hex_detailed_truncated() {
        let data = vec![
            0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64,
        ];
        let result = format_hex_detailed(&data, 5);
        assert!(result.contains("truncated"));
        assert!(result.contains("11 bytes"));
    }
}
