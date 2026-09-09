pub enum Alignment {
    Left,
    Right,
    Center,
}

pub struct Column<R: Row> {
    pub header: &'static str,
    pub alignment: Alignment,
    pub total: Option<fn(&[R]) -> String>,
}

pub trait Row {
    fn columns() -> &'static [Column<Self>] where Self: Sized;
    fn display(&self, index: usize, length: Option<usize>) -> String;
}

pub struct Table<R: Row + 'static> {
    cols: &'static [Column<R>],
}

impl<R: Row> Table<R> {
    pub fn new() -> Self {
        Self {
            cols: R::columns(),
        }
    }

    pub fn render(&self, rows: &[R]) {

    }
}
