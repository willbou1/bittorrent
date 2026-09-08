pub enum Alignment {
    Left,
    Right,
    Center,
}

pub struct Column {
    header: &'static str,
    alignment: Alignment,
    total: bool,
}

pub struct ColumnGroup {
    header: &'static str,
    header_alignment: Alignment,
    columns: Vec<Column>,
}

pub struct Table {
    column_groups: Vec<ColumnGroup>,
}
