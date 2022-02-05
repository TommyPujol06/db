use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum Error {
    NotFound,
    FileError(std::io::Error),
    TableAlreadyExists,
    InvalidColumn,
    InvalidTable,
    Unknown,
}

#[derive(Deserialize, Serialize)]
pub enum DataType {
    Int,
    Float,
    Str,
}

#[derive(Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    pub kind: DataType,
}

impl Column {
    pub fn new<S: Into<String>>(name: S, kind: DataType) -> Self {
        let name = name.into();
        Self { name, kind }
    }
}

#[derive(Deserialize, Serialize)]
pub enum ColumnData {
    Int(Vec<u32>),
    Float(Vec<f64>),
    Str(Vec<String>),
}

#[derive(Deserialize, Serialize)]
pub struct Table {
    pub name: String,
    pub cols: Vec<Column>,
    pub rows: HashMap<String, ColumnData>,
}

impl Table {
    pub fn new<S: Into<String>>(name: S, cols: Vec<Column>) -> Self {
        let name = name.into();
        let mut rows = HashMap::new();

        for col in &cols {
            let col_data = match col.kind {
                DataType::Int => ColumnData::Int(Vec::new()),
                DataType::Float => ColumnData::Float(Vec::new()),
                DataType::Str => ColumnData::Str(Vec::new()),
            };

            rows.insert(col.name.clone(), col_data);
        }

        Self { name, cols, rows }
    }
}

pub struct Database {
    pub path: PathBuf,
    pub file: File,
    pub tables: Vec<Table>,
}

impl Database {
    pub fn new<'a, P>(path: &'a P) -> Self
    where
        P: 'a + ?Sized + AsRef<Path>,
    {
        let path = path.as_ref();
        let mut tables = Vec::new();
        if path.exists() {
            {
                let mut file = File::open(path).unwrap();
                let mut bytes = vec![];
                file.read_to_end(&mut bytes).expect("Could not read file!");

                tables = bincode::deserialize(&bytes[..])
                    .expect("Could not deserialize data! Invalid file format!");
            }
        }
        let file = File::create(path).unwrap();
        Self {
            path: path.to_path_buf(),
            file: file,
            tables: tables,
        }
    }

    pub fn insert<A, B>(&mut self, cols: Vec<A>, values: Vec<A>, table: B) -> Result<(), Error>
    where
        A: Into<String>,
        B: Into<String>,
    {
        if let Some(table) = self.get_table_mut(table) {
            for ((_, col), val) in (0..cols.len()).zip(cols).zip(values) {
                let val = val.into();
                let col = col.into();

                if let Some(col) = table.cols.iter_mut().find(|c| c.name == *col) {
                    if let Some(row) = table.rows.get_mut(&col.name) {
                        match col.kind {
                            DataType::Int => {
                                let val = val.parse::<u32>().unwrap();
                                if let ColumnData::Int(row) = row {
                                    row.push(val);
                                }
                            }
                            DataType::Float => {
                                let val = val.parse::<f64>().unwrap();
                                if let ColumnData::Float(row) = row {
                                    row.push(val);
                                }
                            }
                            DataType::Str => {
                                if let ColumnData::Str(row) = row {
                                    row.push(val);
                                }
                            }
                        }
                    }
                } else {
                    return Err(Error::InvalidColumn);
                }
            }
        } else {
            return Err(Error::InvalidTable);
        }

        Ok(())
    }

    pub fn search<R, A, B>(&self, col: A, val: A, table: B) -> Result<Option<R>, Error>
    where
        A: Into<String>,
        B: Into<String>,
    {
        let _col = col.into();
        let _val = val.into();
        let _table = table.into();

        // if let Some(table) = self.get_table(&table) {
        //     if let Some(row) = table.rows.get(&col) {}
        // } else {
        //     return Err(Error::InvalidTable);
        // }
        //
        Ok(None)
    }

    pub fn update<T>(&mut self, entry: T, table: &mut Table) -> Result<(), Error>
    where
        T: serde::ser::Serialize,
    {
        let _ = entry;
        let _ = table;

        Err(Error::Unknown)
    }

    pub fn insert_table(&mut self, table: Table) -> Result<(), Error> {
        if let Some(_) = self.get_table(&table.name) {
            return Err(Error::TableAlreadyExists);
        }

        self.tables.push(table);
        Ok(())
    }

    pub fn get_table<S: Into<String>>(&self, table: S) -> Option<&Table> {
        let table = table.into();

        self.tables.iter().find(|t| t.name == table)
    }

    pub fn get_table_mut<S: Into<String>>(&mut self, table: S) -> Option<&mut Table> {
        let table = table.into();
        self.tables.iter_mut().find(|t| t.name == table)
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        let data = &self.tables;

        let bytes: Vec<u8> = bincode::serialize(&data).unwrap();
        match self
            .file
            .write_all(&bytes)
            .and_then(|_| self.file.sync_data())
        {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::FileError(e)),
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        match self.flush() {
            Ok(()) => {}
            Err(e) => eprintln!("Error: {:?}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn test_insert() {
        let mut db = Database::new("./test1.db");
        let people = Table::new(
            "people",
            vec![
                Column::new("name", DataType::Str),
                Column::new("age", DataType::Int),
            ],
        );

        assert_eq!(db.insert_table(people).is_err(), false);

        let table = "people";
        let columns = vec!["name", "age"];
        let data = vec!["Tommy", "16"];

        assert_eq!(db.insert(columns, data, table).is_err(), false);
    }

    #[test]
    fn test_search() {
        let mut db = Database::new("./test2.db");
        let people = Table::new(
            "people",
            vec![
                Column::new("name", DataType::Str),
                Column::new("age", DataType::Int),
            ],
        );

        assert_eq!(db.insert_table(people).is_err(), false);

        let table = "people";
        let columns = vec!["name", "age"];
        let data = vec!["Tommy", "16"];

        assert_eq!(db.insert(columns, data, table).is_err(), false);

        assert_eq!(
            db.search::<Option<String>, &str, &str>("name", "Tommy", "people")
                .unwrap()
                .is_none(),
            false
        );
        assert_eq!(
            db.search::<Option<String>, &str, &str>("name", "Tomàs", "people")
                .unwrap()
                .is_none(),
            true
        );
    }

    #[test]
    fn test_duplicate_tables() {
        let mut db = Database::new("./test3.db");
        let people1 = Table::new(
            "people",
            vec![
                Column::new("name", DataType::Str),
                Column::new("age", DataType::Int),
            ],
        );

        let people2 = Table::new(
            "people",
            vec![
                Column::new("name", DataType::Str),
                Column::new("age", DataType::Int),
            ],
        );

        assert_eq!(db.insert_table(people1).is_err(), false);
        assert_eq!(db.insert_table(people2).is_err(), true);
    }
}
