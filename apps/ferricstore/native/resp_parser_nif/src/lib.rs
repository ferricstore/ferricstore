#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::explicit_auto_deref)]

include!("sections/part_01.rs");
include!("sections/part_02.rs");
include!("sections/part_03.rs");
include!("sections/part_04.rs");
include!("sections/part_05.rs");
include!("sections/part_06.rs");
include!("sections/part_07.rs");
include!("sections/part_08.rs");
include!("sections/part_09.rs");
include!("sections/part_10.rs");
include!("sections/part_11.rs");
include!("sections/part_12.rs");

#[cfg(test)]
mod tests {
    include!("sections/tests_part_01.rs");
    include!("sections/tests_part_02.rs");
}
