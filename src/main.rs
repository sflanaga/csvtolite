use std::fs::{metadata, read_dir, symlink_metadata, FileType, Metadata};
use std::process::exit;

use anyhow::{anyhow, Context, Result, Error};
use grep_cli::DecompressionReader;
use regex::Captures;
use regex::Regex;

use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;

use rusqlite::{NO_PARAMS, MappedRows, Statement};
use rusqlite::{Column, Connection, Row};
use lazy_static::lazy_static;
use csv::{StringRecord, ByteRecord};
use regex::internal::Input;

//
// handle verbose logging succinctly - not a lot of fuss here
//
macro_rules! mylog {
    ($level:expr, $fmt:expr) => {
        {
            if CLI.verbose >= (($level) as usize) {
                eprintln!("{:?}", $fmt);
            }
        }
    };
    ($level:expr, $fmt:expr, $($arg:tt)+) => {
        {
            if CLI.verbose >= (($level) as usize) {
                eprintln!($fmt, $($arg)+);
            }
        }
    }
}



fn main() -> Result<()> {
    import_csv()?;
//    match import_csv()  {
//        Err(err) => {
//            eprintln!("ERROR: {}", &err);
//            if let Some(derr) = err.downcast_ref::<anyhow::Error>() {
//                eprintln!("does have trace??");
//            }
//        },
//        _ => {},
//    }
    Ok(())
}

fn parse_char_into_u8(src: &str) -> Result<u8> {
    Ok(src.as_bytes()[0])
}


#[derive(StructOpt, Debug, Clone)]
#[structopt(
global_settings(& [structopt::clap::AppSettings::ColoredHelp, structopt::clap::AppSettings::VersionlessSubcommands, structopt::clap::AppSettings::DeriveDisplayOrder]),
//raw(setting = "structopt::clap::AppSettings::DeriveDisplayOrder"),
author, about
)]
///
/// import csv files into sqlite3
///
pub struct CliCfg {
    #[structopt(short = "f", name = "file", parse(from_os_str))]
    /// list of input files
    pub files: Vec<PathBuf>,

    #[structopt(short = "r", name = "filere")]
    /// regex to parse filename to find tablename
    pub file_re: String,

    #[structopt(short = "d", name = "db", parse(from_os_str))]
    /// existing database to import into
    pub db_file: PathBuf,

    #[structopt(short = "v", parse(from_occurrences))]
    /// Verbosity - use more than one v for greater detail
    pub verbose: usize,

    #[structopt(short = "s", parse(try_from_str = parse_char_into_u8), default_value(","))]
    /// Field seperator used in the csv file
    pub field_sep: u8,

    #[structopt(long = "quote", parse(try_from_str = parse_char_into_u8), default_value("\""))]
    /// Quote use for field parsing
    pub quote: u8,

    #[structopt(long = "escape", parse(try_from_str = parse_char_into_u8))]
    /// Escape use for field parsing - typically a \ but can be faster without
    pub escape: Option<u8>,

    #[structopt(long = "comment", parse(try_from_str = parse_char_into_u8))]
    /// Comment character for lines to skip in csv
    pub comment: Option<u8>,

    #[structopt(long = "headeron")]
    /// Cut ON header parsing for field name
    ///
    /// Defaults to just using f1, f2, f3 as table fieldnames
    pub headeron: bool,


}

fn get_cli() -> anyhow::Result<CliCfg> {
    let ccfg = {
        let mut ccfg: CliCfg = CliCfg::from_args();
        for f in &ccfg.files {
            if !f.exists() || !f.is_file() {
                return Err(anyhow!(
                    "CSV File does not exist or is not a file: {}",
                    &f.display()
                ));
            }
        }
        if !ccfg.db_file.exists() || !ccfg.db_file.is_file() {
            return Err(anyhow!(
                "Database file does not exist or is not a file: {}",
                &ccfg.db_file.display()
            ));
        }

        let re = Regex::new(&ccfg.file_re)?;
        if ccfg.verbose > 0 {
            eprintln!("Cli cfg: {:#?}", ccfg);
        }
        ccfg
    };
    Ok(ccfg)
}

lazy_static! {
    static ref CLI: CliCfg = {
       match get_cli() {
            Err(e) => {
                eprintln!("Error in cli options: {}", e);
                exit(1);
            }
            Ok(c) => c,
       }
    };
}


fn get_table_name(path: &PathBuf) -> Result<String> {
    Ok(String::from(
        Regex::new(&CLI.file_re)?
            .captures(path.to_str().expect("unable to convert path to &str"))
            .expect(&format!(
                "was not able to match path to file re {}",
                path.display()
            ))
            .get(1)
            .expect(&format!(
                "sub group from file re did not match or is not available: {}",
                path.display()
            ))
            .as_str(),
    ))

    // let re = Regex::new(&cfg.file_re)?;
    // let caps = re.captures(path.to_str().unwrap()).unwrap();
    // if caps.len() <= 2 {
    //     return Err(anyhow!("path does not match re and cannot find sub gropus"));
    // } else {
    //     return Ok(String::from(caps.get(1).unwrap().as_str()));
    // }
}

fn _does_table_exist(conn: &Connection, tablename: &str) -> Result<Vec<Vec<String>>> {
    let sql = format!("pragma table_info({});", &tablename);
    println!("running sql: {}", &sql);
    let mut fields: Vec<Vec<String>> = Vec::new();
    let mut stmt = conn.prepare(sql.as_str())?;
    let rows = stmt.query_map(NO_PARAMS, |row| {
        let mut v = Vec::new();
        for i in 0..row.column_count() {
            let f = row.get(i)?;
            v.push(f);
        }
        Ok(v)
    })?;
    for row in rows {
        fields.push(row?);
    }
    Ok(fields)
}

#[derive(Debug)]
struct Field {
    pos: u32,
    name: String,
    db_type: String,
}

fn schema(conn: &Connection, tablename: &str) -> Result<Vec<Field>> {
    let sql = format!("pragma table_info({});", &tablename);
    println!("running sql: {}", &sql);
    let mut fields = Vec::new();
    let mut stmt = conn.prepare(sql.as_str())?;
    let mut rows = stmt.query(NO_PARAMS)?;
    while let Some(row) = rows.next()? {
        let row: &Row = row;
        let f = Field {
            pos: row.get(0)?,
            name: row.get(1)?,
            db_type: row.get(2)?,
        };
        fields.push(f);
    }

    Ok(fields)
}



fn import_csv() -> Result<()> {
    let conn = Connection::open(&CLI.db_file)?;

    for pathbuf in &CLI.files {
        load_file(&CLI, &conn, &pathbuf)?;
    }

    Ok(())
}

fn load_file(cfg: &CliCfg, conn: &Connection, pathbuf: &PathBuf) -> Result<()> {
    let tablename = get_table_name(&pathbuf)?;
    mylog!(1, "tablename: {} from file: {}", &tablename, &pathbuf.display());

    let table_schema = schema(&conn, &tablename)?;
    mylog!(2, "table schema: {:#?}", &table_schema);

    let file_schema = detect_file_schema(pathbuf)?;
    mylog!(2, "file schema, file: {}, schema: {:#?}", &pathbuf.display(), &file_schema);

    if table_schema.len() == 0 {
        // create table
        create_table(&conn, &tablename, &file_schema)?;

    } else {
        //
        // compare db schema vs file schema
        //
        if table_schema.len() != file_schema.len() {
            return Err(anyhow!("Schema diff in number of fields: table fields {} vs file field {}  table: {}  file: {}", table_schema.len(), file_schema.len(), &tablename, &pathbuf.display()));
        }
        for cmp in table_schema.iter().zip(file_schema.iter()) {
            let tmp: (&Field, &Field) = cmp;
            if tmp.0.name != tmp.1.name {
                return Err(anyhow!("Schema diff in name: table field {} vs file field {}  table: {}  file: {}", &tmp.0.name, &tmp.1.name, &tablename, &pathbuf.display()));
            } else if tmp.0.db_type != tmp.1.db_type {
                return Err(anyhow!("Schema diff in type: table field {} vs file field {}  table: {}  file: {}", &tmp.0.db_type, &tmp.1.db_type, &tablename, &pathbuf.display()));
            }
        }
    }

    //
    // load data
    //
    write_to_db(&conn, &pathbuf, &tablename,&file_schema)?;


    Ok(())
}

fn detect_file_schema(pathbuf: &PathBuf) -> Result<Vec<Field>> {
    let mut rdr = match DecompressionReader::new(&pathbuf) {
        Ok(rdr) => rdr,
        Err(err) => Err(anyhow!("Cannot read file \"{}\", due to error: {}", pathbuf.display(), err))?,
    };
    let mut builder = csv::ReaderBuilder::new();
    builder.delimiter(CLI.field_sep).has_headers(false).flexible(true).escape(CLI.escape).quote(CLI.quote as u8).comment(CLI.comment);

    let mut schema = vec![];

    let mut rec_rdr =builder.from_reader(rdr);
    let mut line_count = 0;

    let mut field_count = 0;

    for record in rec_rdr.records() {
        let record = record?;
        line_count += 1;
        if line_count == 1 {
            if CLI.headeron {
                // this first line is the header to gather that info
                for (i, f) in record.iter().enumerate() {
                    let f = Field {
                        pos: i as u32,
                        name: f.to_string(),
                        db_type: "text".to_string(),
                    };
                    println!("insert {:?}", f);
                    schema.push(f);
                }

            }
            field_count = record.len();
        } else {
            if record.len() != field_count {
                return Err(anyhow!("Field count inconsistency: line: {}  field count: {}  expected field count: {}  file: {}", line_count, record.len(), field_count, &pathbuf.display()));
            }
        }
        if line_count > 10 {
            break;
        }
    }

    if CLI.headeron {
        if field_count == 0 {
            return Err(anyhow!("Did not field headers so cannot schema-check on file: {}", pathbuf.display()));
        }
        return Ok(schema);
    } else {
        if field_count == 0 {
            return Err(anyhow!("Did not find anything (empty?) to schema-check on file: {}", pathbuf.display()));
        } else {
            for i in 0..field_count {
                schema.push(
                    Field {
                        pos: i as u32,
                        name: format!("f{}", i),
                        db_type: "text".to_string(),
                    }
                );
            }
        }
    }

    Ok(schema)
}




fn write_to_db(conn: &Connection, pathbuf: &PathBuf, tablename: &str, f_sch: &Vec<Field>) -> Result<(u64, u64)> {
    let mut rdr = match DecompressionReader::new(&pathbuf) {
        Ok(rdr) => rdr,
        Err(err) => Err(anyhow!("Cannot read file \"{}\", due to error: {}", pathbuf.display(), err))?,
    };
    let mut builder = csv::ReaderBuilder::new();
    builder.delimiter(CLI.field_sep).has_headers(false).flexible(true).escape(CLI.escape).quote(CLI.quote as u8).comment(CLI.comment);
    let mut rec_rdr =builder.from_reader(rdr);
    let mut line_count = 0;
    let mut sql = format!("insert into {} ( {} ) \nvalues( {} );", &tablename,
                          f_sch.iter().map(|f| f.name.as_str()).collect::<Vec<&str>>().join(", "),
                          f_sch.iter().enumerate().map(|(i, e)| format!("?{}", i + 1)).collect::<Vec<String>>().join(", "));

    let mut stmt = conn.prepare(&sql).with_context(|| format!("Sql used: {}", &sql))?;
    mylog!(2, "SQL for load: {}", &sql);
    conn.execute_batch("begin transaction;")?;

    let mut x_complete = std::cell::Cell::new(false);
    use scopeguard::defer;

    defer! {{
        mylog!(1, "in defer of write_to_db");
        if !x_complete.get() {
            mylog!(1, "rollback in defer for write_to_db");

            if let Err(e) = conn.execute_batch("rollback;") {
                mylog!(0, "There was a problem with deferal rollback: {}", e);
            }
        }
    }};

    let (mut row_count, mut field_count) = (0u64, 0u64);
    for record in rec_rdr.records() {
        let record = record?;
        line_count += 1;
        if line_count == 1 && CLI.headeron {
            // skip this line and assume it was already checked header vs schema

        } else {
            // we know that stmt must be set by now
            stmt.execute(&record)?;
            row_count +=1;
            field_count += f_sch.len() as u64;
        }
        // TODO:
    }
    conn.execute_batch("commit;")?;
    x_complete.set(true);

    Ok((row_count,field_count))
}

fn create_table(conn: &Connection, tablename: &str, f_sch: &Vec<Field>) -> Result<()> {

    let mut sql = format!("create table {} (\n", tablename);
    for f in f_sch.iter().take(f_sch.len()-1) {
        sql.push_str(&format!("\t{} {},", f.name, f.db_type));
    }
    let last: &Field = f_sch.iter().rev().nth(0).unwrap();
    sql.push_str(&format!("\t{} {}\n);", last.name, last.db_type));
    mylog!(1, "Executing create sql: {}", &sql);

    conn.execute(sql.as_str(), NO_PARAMS)?;

    Ok(())
}