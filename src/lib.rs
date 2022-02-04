use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum Error {
    NotFound,
    FileError(std::io::Error),
    TableAlreadyExists,
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
    Int(u32),
    Float(f64),
    Str(String),
}

#[derive(Deserialize, Serialize)]
pub struct Table {
    pub name: String,
    pub cols: Vec<Column>,
}

impl Table {
    pub fn new<S: Into<String>>(name: S, cols: Vec<Column>) -> Self {
        let name = name.into();
        Self { name, cols }
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
        let file = File::create(path).unwrap();
        Self {
            path: path.to_path_buf(),
            file: file,
            tables: Vec::new(), // TODO: Load tables from file if it exists.
        }
    }

    pub fn insert<T, S>(&self, data: T, table: S) -> Result<(), Error>
    where
        T: serde::ser::Serialize,
        S: Into<String>,
    {
        let _ = data;
        let _ = table;

        Err(Error::Unknown)
    }

    pub fn query<T, S>(&self, entry: T, table: S) -> Result<T, Error>
    where
        T: serde::ser::Serialize,
        S: Into<String>,
    {
        let _ = entry;
        let _ = table;

        Err(Error::Unknown)
    }

    pub fn update<T, S>(&self, entry: T, table: S) -> Result<(), Error>
    where
        T: serde::ser::Serialize,
        S: Into<String>,
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
        let mut db = Database::new("./test.db");
        let people = Table::new(
            "people",
            vec![
                Column::new("name", DataType::Str),
                Column::new("age", DataType::Int),
            ],
        );

        assert_eq!(db.insert_table(people).is_err(), false);

        let data = vec![ColumnData::Str(String::from("Tommy")), ColumnData::Int(16)];
        assert_eq!(db.insert(data, "people").is_err(), true);
    }

    #[test]
    fn duplicate_tables() {
        let mut db = Database::new("./test.db");
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
