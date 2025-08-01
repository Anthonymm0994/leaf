use csv::{Reader, Writer, StringRecord, ReaderBuilder};
use std::fs::File;
use std::path::Path;
use std::io::BufReader;
use crate::core::error::Result;
use std::path::PathBuf;

pub struct CsvReader {
    path: PathBuf,
    delimiter: u8,
    reader: Option<Reader<BufReader<File>>>,
}

impl CsvReader {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self { 
            path: path.as_ref().to_path_buf(),
            delimiter: b',',
            reader: None,
        })
    }
    
    pub fn set_delimiter(&mut self, delimiter: char) {
        self.delimiter = delimiter as u8;
        // Reset reader if delimiter changes
        self.reader = None;
    }
    
    fn build_reader(&self) -> Result<Reader<BufReader<File>>> {
        let file = File::open(&self.path)?;
        let reader = ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(false)
            .from_reader(BufReader::new(file));
        Ok(reader)
    }
    
    fn get_or_create_reader(&mut self) -> Result<&mut Reader<BufReader<File>>> {
        if self.reader.is_none() {
            self.reader = Some(self.build_reader()?);
        }
        Ok(self.reader.as_mut().unwrap())
    }
    
    pub fn headers(&mut self) -> Result<Vec<String>> {
        let mut reader = self.build_reader()?;
        if let Some(result) = reader.records().next() {
            Ok(result?.iter().map(|s| s.to_string()).collect())
        } else {
            Ok(Vec::new())
        }
    }
    
    pub fn records(&mut self) -> Result<Vec<StringRecord>> {
        let mut reader = self.build_reader()?;
        let mut records = Vec::new();
        for result in reader.records() {
            records.push(result?);
        }
        Ok(records)
    }
    
    pub fn sample_records(&mut self, n: usize) -> Result<Vec<StringRecord>> {
        let mut reader = self.build_reader()?;
        let mut records = Vec::new();
        for (i, result) in reader.records().enumerate() {
            if i >= n {
                break;
            }
            records.push(result?);
        }
        Ok(records)
    }
    
    pub fn read_record(&mut self) -> Result<Option<StringRecord>> {
        let reader = self.get_or_create_reader()?;
        if let Some(result) = reader.records().next() {
            Ok(Some(result?))
        } else {
            Ok(None)
        }
    }
}

pub struct CsvWriter {
    writer: Writer<File>,
}

impl CsvWriter {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::create(path)?;
        let writer = csv::Writer::from_writer(file);
        Ok(Self { writer })
    }
    
    pub fn write_headers(&mut self, headers: &[String]) -> Result<()> {
        self.writer.write_record(headers)?;
        Ok(())
    }
    
    pub fn write_record(&mut self, record: &[String]) -> Result<()> {
        self.writer.write_record(record)?;
        Ok(())
    }
    
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
} 