pub enum Alignment {
    Left,
    Right,
}

enum Junction {
    Top,
    Center,
    Bottom,
}

pub struct Column<R: Row> {
    pub header: &'static str,
    pub alignment: Alignment,
    pub max_width: Option<usize>,
    pub flex: Option<usize>,
    pub total: Option<fn(&[R]) -> String>,
}

pub trait Row {
    fn columns() -> &'static [Column<Self>] where Self: Sized;
    fn display_column(&self, index: usize, length: Option<usize>) -> String;
}

pub struct Table<R: Row + 'static> {
    columns: &'static [Column<R>],
    flex_total: usize,
    padding: usize,
}

impl<R: Row> Table<R> {
    pub fn new(padding: usize) -> Self {
        Self {
            columns: R::columns(),
            padding,
            flex_total: R::columns().iter()
                .map(|c| c.flex)
                .fold(0, |t, f| t + f.unwrap_or(0)),
        }
    }

    fn render_cell(&self, c: usize, cell: &str, width: usize) -> String {
        match self.columns[c].alignment {
            Alignment::Left => format!("{cell:<width$}"),
            Alignment::Right => format!("{cell:>width$}"),
        }
    }

    fn render_sparator(&self, widths: &[usize], junction: Junction) -> String {
        let mut render = String::from(match junction {
            Junction::Top => "╭",
            Junction::Center => "├",
            Junction::Bottom => "╰",
        });
        for c in 0..self.columns.len() {
            render.extend(std::iter::repeat_n('─', self.padding * 2 + widths[c]));
            if c != self.columns.len() - 1 && self.columns.len() != 0 {
                match junction {
                    Junction::Top => render.push('┬'),
                    Junction::Center => render.push('┼'),
                    Junction::Bottom => render.push('┴'),
                }
            }
        }
        render.push(match junction {
            Junction::Top => '╮',
            Junction::Center => '┤',
            Junction::Bottom => '╯',
        });
        render
    }

    fn render_cells(&self, cells: &[String], widths: &[usize]) -> String {
        let mut render = String::from("│");
        for (c, col) in self.columns.iter().enumerate() {
            render.extend(std::iter::repeat_n(' ', self.padding));
            render.push_str(
                &self.render_cell(c, &cells[c], widths[c])
            );
            render.extend(std::iter::repeat_n(' ', self.padding));
            render.push('│');
        }
        render
    }

    fn chop_string(&self, c: usize, cell: &str) -> String {
        if let Some(max_width) = self.columns[c].max_width {
            cell.chars().take(max_width).chain(['…']).collect()
        } else {
            cell.to_string()
        }
    }

    pub fn render(&self, rows: &[R], width: usize) -> String {
        let mut render = String::new();
        let mut filled_columns: Vec<Vec<_>> = Vec::new();
        for (c, col) in self.columns.iter().enumerate() {
            let mut filled_column = vec![col.header.to_string()];
            filled_column.extend(
                rows.iter().map(|r| {
                    if col.flex.is_none() {
                        self.chop_string(c, &r.display_column(c, None))
                    } else {
                        String::new()
                    }
                })
            );
            filled_column.push(
                col.total.map(|t| t(rows)).unwrap_or(String::new())
            );
            filled_columns.push(filled_column);
        }
        // Natural width of every column.
        let natural_widths: Vec<usize> = filled_columns
            .iter()
            .map(|fc| {
                fc.iter()
                    .map(|cell| cell.chars().count()).max().unwrap_or(0)
            })
            .collect();

        let taken_width: usize = natural_widths.iter()
            .zip(self.columns.iter())
            .filter(|(_, col)| col.flex.is_none())
            .map(|(width, _)| *width)

            .sum();

        let separator_width = (2 * self.padding + 1) * self.columns.len() + 1;

        let mut column_widths = natural_widths;

        let mut remaining = width.saturating_sub(taken_width + separator_width);
        let mut remaining_flex = self.flex_total;

        for (c, col) in self.columns.iter().enumerate() {
            if let Some(flex) = col.flex {
                let column_width =
                    ((remaining as u128 * flex as u128) / remaining_flex as u128) as usize;

                column_widths[c] = column_width;
                if col.flex.is_some() {
                    filled_columns[c][1..(rows.len() + 2)]
                        .iter_mut()
                        .zip(rows.iter())
                        .for_each(|(cell, row)| {
                            *cell = row.display_column(c, Some(column_widths[c]));
                        });
                }
                remaining -= column_width;
                remaining_flex -= flex;
            }
        }
        render.push_str(&self.render_sparator(&column_widths, Junction::Top));
        render.push('\n');
        render.push_str(&self.render_cells(
                &self.columns.iter().map(|c| c.header.to_string()).collect::<Vec<_>>(),
                &column_widths
        ));
        render.push('\n');
        render.push_str(&self.render_sparator(&column_widths, Junction::Center));
        render.push('\n');
        for r in 0..rows.len() {
            render.push_str(&self.render_cells(
                &filled_columns.iter().map(|c| c[r + 1].clone()).collect::<Vec<_>>(),
                &column_widths
            ));
            if r != rows.len() {
                render.push('\n');
            }
        }
        render.push_str(&self.render_sparator(&column_widths, Junction::Center));
        render.push('\n');
        render.push_str(&self.render_cells(
            &filled_columns.iter().map(|c| c[rows.len() + 1].clone()).collect::<Vec<_>>(),
            &column_widths
        ));
        render.push('\n');
        render.push_str(&self.render_sparator(&column_widths, Junction::Bottom));
        render.push('\n');
        render
    }
}
