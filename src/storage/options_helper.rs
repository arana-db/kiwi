use std::num::ParseIntError;

#[derive(Debug)]
pub enum MemberType {
    Int,
    Uint,
    Uint64T,
    SizeT,
}

fn str_to_int(value: &str, base: u32) -> Result<i32, ParseIntError> {
    i32::from_str_radix(value.trim(), base)
}

fn str_to_uint64(value: &str, base: u32) -> Result<u64, ParseIntError> {
    u64::from_str_radix(value.trim(), base)
}

fn str_to_uint32(value: &str, base: u32) -> Result<u32, Box<dyn std::error::Error>> {
    let uint64_val = str_to_uint64(value, base)?;
    if uint64_val <= u32::MAX as u64 {
        Ok(uint64_val as u32)
    } else {
        Err(format!("out of range: {}", value).into())
    }
}

pub fn parse_option_member(
    member_type: MemberType,
    value: &str,
    member_address: &mut dyn std::any::Any,
) -> bool {
    match member_type {
        MemberType::Int => {
            if let Ok(val) = str_to_int(value, 10) {
                if let Some(int_ref) = member_address.downcast_mut::<i32>() {
                    *int_ref = val;
                    return true;
                }
            }
        }
        MemberType::Uint => {
            if let Ok(val) = str_to_uint32(value, 10) {
                if let Some(uint_ref) = member_address.downcast_mut::<u32>() {
                    *uint_ref = val;
                    return true;
                }
            }
        }
        MemberType::Uint64T => {
            if let Ok(val) = str_to_uint64(value, 10) {
                if let Some(uint64_ref) = member_address.downcast_mut::<u64>() {
                    *uint64_ref = val;
                    return true;
                }
            }
        }
        MemberType::SizeT => {
            if let Ok(val) = str_to_uint64(value, 10) {
                if let Some(size_ref) = member_address.downcast_mut::<usize>() {
                    *size_ref = val as usize;
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_option_member() {
        let mut int_value: i32 = 0;
        assert!(parse_option_member(MemberType::Int, "42", &mut int_value));
        assert_eq!(int_value, 42);

        let mut uint_value: u32 = 0;
        assert!(parse_option_member(MemberType::Uint, "42", &mut uint_value));
        assert_eq!(uint_value, 42);

        let mut uint64_value: u64 = 0;
        assert!(parse_option_member(
            MemberType::Uint64T,
            "42",
            &mut uint64_value
        ));
        assert_eq!(uint64_value, 42);

        let mut size_value: usize = 0;
        assert!(parse_option_member(
            MemberType::SizeT,
            "42",
            &mut size_value
        ));
        assert_eq!(size_value, 42);
    }
}
