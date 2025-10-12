#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BooleanMode {
    Integer,
    SimpleString,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DoubleMode {
    BulkString,
    IntegerIfWhole,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapMode {
    FlatArray,
    ArrayOfPairs,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DownlevelPolicy {
    pub boolean_mode: BooleanMode,
    pub double_mode: DoubleMode,
    pub map_mode: MapMode,
}

impl Default for DownlevelPolicy {
    fn default() -> Self {
        Self {
            boolean_mode: BooleanMode::Integer,
            double_mode: DoubleMode::BulkString,
            map_mode: MapMode::FlatArray,
        }
    }
}
