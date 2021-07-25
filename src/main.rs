use log::{trace, debug, info, warn, error};
use simplelog::*;

use std::fs::{metadata, read_dir, symlink_metadata, FileType, Metadata};
use std::process::exit;

use anyhow::{anyhow, Context, Error, Result};
use grep_cli::DecompressionReader;
use regex::Regex;

use std::path::PathBuf;
use structopt::StructOpt;

use csv::{StringRecord, StringRecordsIntoIter};
use lazy_static::lazy_static;
use rusqlite::{Connection, OpenFlags, Params, Row, params};
use rusqlite::{Statement, NO_PARAMS};
use rusqlite::types::Value;
use rusqlite::types::Type::{Null, Integer, Text, Blob};


fn main() -> Result<()> {
    import_csv()?;
    Ok(())
}

fn parse_char_into_u8(src: &str) -> Result<u8> {
    Ok(src.as_bytes()[0])
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(
global_settings(& [structopt::clap::AppSettings::ColoredHelp, structopt::clap::AppSettings::VersionlessSubcommands, structopt::clap::AppSettings::DeriveDisplayOrder]),
//raw(setting = "structopt::clap::AppSettings::DeriveDisplayOrder"),
author, about=r"
Import csv files into sqlite3

e.g. csv2lite -f flights_1.csv flights_2.csv -r '^(.+)_\d+\.csv' -s '|' -d mytest.db
    writes flight data into a table called flights into mytest.db sqlite3 database
"
)]
pub struct CliCfg {
    #[structopt(short = "f", name = "file", parse(from_os_str))]
    /// list of input files
    pub files: Vec<PathBuf>,

    #[structopt(short = "r", name = "filere")]
    /// regex to parse tablename out of the filename(s) using the 1st sub group
    pub file_re: Option<String>,

    #[structopt(short = "t", name = "tablename")]
    /// Tablename into which to write data
    pub tablename: Option<String>,

    #[structopt(short = "d", name = "open_db", parse(from_os_str))]
    /// existing database to import into
    pub db_file: PathBuf,

    #[structopt(short = "v", parse(from_occurrences))]
    /// Verbosity - use more than one v for greater detail
    pub verbose: usize,

    #[structopt(short = "s", parse(try_from_str = parse_char_into_u8), default_value(","))]
    /// Field seperator used in the csv file
    pub field_sep: u8,

    #[structopt(short = "o", name = "overwrite_tables")]
    /// Replace tables if they alread exist via a "drop"
    pub overwrite_tables: bool,

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
    /// Cut ON header parsing for field name in table.
    ///
    /// Defaults to just using f1, f2, f3 as table field names.  It uses the first line as the
    /// guide for this.
    pub headeron: bool,

    #[structopt(long = "sanity_sample", default_value("0"))]
    /// Number of rows to sanity check against header and schema - zero means all and is the default
    pub sanity_sample: u64,

    #[structopt(long = "ignore_field_count")]
    /// Allow import of records that have different number of fields from header
    /// or table
    pub ignore_field_count: bool,

    #[structopt(long = "sqls")]
    /// Run 1 or more sql after the import.  Good especially for memory based DBs.
    pub sqls: Vec<String>,

    #[structopt(long = "out_delimiter", default_value(","))]
    /// Run 1 or more sql after the import.  Good especially for memory based DBs.
    pub out_delimiter: String,

    #[structopt(long = "memory")]
    /// Create the database in memory - note use with --sqls as it will disappear
    pub memory: bool,

}

fn get_cli() -> anyhow::Result<CliCfg> {
    let ccfg = {
        let mut ccfg: CliCfg = CliCfg::from_args();
        let lvl = match ccfg.verbose {
            3 => LevelFilter::Trace,
            2 => LevelFilter::Debug,
            1 => LevelFilter::Info,
            _ => LevelFilter::Warn,
        };
        let _ = SimpleLogger::init(lvl, Config::default());
        for f in &ccfg.files {
            if !f.exists() || !f.is_file() {
                return Err(anyhow!(
                    "CSV File does not exist or is not a file: {}",
                    &f.display()
                ));
            }
        }
        if !ccfg.db_file.exists() {
            warn!("No existing data so creating new one at file: {}", ccfg.db_file.display());
        } else if !ccfg.db_file.is_file() {
            return Err(anyhow!(
                "File {} is not a file",
                &ccfg.db_file.display()));
        }

        if ccfg.file_re.is_none() == ccfg.tablename.is_none() {
            return Err(anyhow!(
                "One of file_re or tablename must be specified and not both"
            ));
        }

        if let Some(file_re) = &ccfg.file_re {
            let re = Regex::new(file_re)?;
        }
        trace!("Cli cfg: {:#?}", ccfg);
        //Logger::init(TermLogger::new(lvl, Config::default(), TerminalMode::Stderr).unwrap());
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
    if let Some(file_re) = &CLI.file_re {
        Ok(String::from(
            Regex::new(file_re.as_str())?
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
    } else {
        Ok(String::from(CLI.tablename.as_ref().unwrap()))
    }
}

fn _does_table_exist(conn: &Connection, tablename: &str) -> Result<Vec<Vec<String>>> {
    let sql = format!("pragma table_info({});", &tablename);
    debug!("running sql: {}", &sql);
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

fn schema(cfg: &CliCfg, conn: &Connection, tablename: &str) -> Result<Vec<Field>> {
    let sql = if cfg.overwrite_tables {
        format!("drop table {};", &tablename)
    } else {
        format!("pragma table_info({});", &tablename)
    };

    warn!("executing sql: {}", sql);

    let mut fields = Vec::new();
    let mut stmt = match conn.prepare(sql.as_str()) {
        Err(e) => return if cfg.overwrite_tables {
            // here the drop fails and we think it can be ignored
            // because it just means it is not there and there
            // is nothing to drop anyway.
            warn!("Overwrite set so this error during drop table ignored: \"{}\"", e);
            Ok(fields)
        } else {
            Err(anyhow!(format!("error during schema check: {}" ,e)))
        },
        Ok(s) => s,
    };

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
    trace!("opening DB");

    let conn = if CLI.memory {
        warn!("opening in memory only");
        Connection::open_in_memory()?
    } else {
        Connection::open(&CLI.db_file)?
    };

    trace!("conn created starting loads");
    for pathbuf in &CLI.files {
        load_file(&CLI, &conn, &pathbuf)?;
    }

    run_post_sqls(&conn)?;
    Ok(())
}

fn val_append(s: &mut String, val: Value) {
    match val {
        Value::Null => s.push_str("NULL"),
        Value::Integer(v) => s.push_str(&format!("{}", v)),
        Value::Text(v) => s.push_str(&format!("{}", v)),
        Value::Blob(v) => s.push_str(&format!("..BLOB..")),
        Value::Real(v) => s.push_str(&format!("{}", v)),
    }
}

fn run_post_sqls(conn: &Connection) -> Result<()> {
    for sql in CLI.sqls.iter() {
        let mut stmt = conn.prepare(&sql)?;
        let mut sb = String::new();

        // todo!("this whole thing is silly and should be redone more elegantly");
        for i in 0 .. stmt.column_count()-1 {
            print!("{}{}",stmt.column_names()[i], &CLI.out_delimiter);
        }
        println!("{}", stmt.column_names()[stmt.column_count()-1]);

        let mut rows = stmt.query(NO_PARAMS)?;
        while let Some(row) = rows.next()? {
            let row: &Row = row;
            for i in 0 .. row.column_count()-1 {
                let x: Value = row.get(i)?;
                val_append(&mut sb, x);
                sb.push_str(&CLI.out_delimiter);
            }
            {
                let x: Value = row.get(row.column_count()-1)?;
                val_append(&mut sb, x);
                println!("{}", &sb);
                sb.clear();
            }
        }
    }

    Ok(())
}


fn load_file(cfg: &CliCfg, conn: &Connection, pathbuf: &PathBuf) -> Result<()> {
    let tablename = get_table_name(&pathbuf)?;
    warn!(
        "tablename: {} from file: {}",
        &tablename,
        &pathbuf.display()
    );
    let start = std::time::Instant::now();

    let table_schema = schema(&cfg, &conn, &tablename)?;
    trace!("table schema: {:#?}", &table_schema);

    let file_schema = detect_file_schema(pathbuf)?;
    trace!(
        "file schema, file: {}, schema: {:#?}",
        &pathbuf.display(),
        &file_schema
    );

    if table_schema.len() == 0 {
        // create table
        create_table(&conn, &tablename, &file_schema)?;
    } else {
        //
        // compare db schema vs file schema
        //
        if !CLI.ignore_field_count && table_schema.len() != file_schema.len() {
            return Err(anyhow!("Schema diff in number of fields: table fields {} vs file field {}  table: {}  file: {}", table_schema.len(), file_schema.len(), &tablename, &pathbuf.display()));
        }
        for cmp in table_schema.iter().zip(file_schema.iter()) {
            let tmp: (&Field, &Field) = cmp;
            if tmp.0.name != tmp.1.name {
                return Err(anyhow!(
                    "Schema diff in name: table field {} vs file field {}  table: {}  file: {}",
                    &tmp.0.name,
                    &tmp.1.name,
                    &tablename,
                    &pathbuf.display()
                ));
            } else if tmp.0.db_type != tmp.1.db_type {
                return Err(anyhow!(
                    "Schema diff in type: table field {} vs file field {}  table: {}  file: {}",
                    &tmp.0.db_type,
                    &tmp.1.db_type,
                    &tablename,
                    &pathbuf.display()
                ));
            }
        }
    }

    //
    // load data
    //
    let (rows, fields) = write_to_db(&conn, &pathbuf, &tablename, &file_schema)?;

    warn!("Loaded {}/{} rows/fields into \"{}\" in {:.3} seconds", rows, fields, &tablename, start.elapsed().as_secs_f64());

    Ok(())
}

fn detect_file_schema(pathbuf: &PathBuf) -> Result<Vec<Field>> {
    let mut rdr = match DecompressionReader::new(&pathbuf) {
        Ok(rdr) => rdr,
        Err(err) => Err(anyhow!(
            "Cannot read file \"{}\", due to error: {}",
            pathbuf.display(),
            err
        ))?,
    };
    let mut builder = csv::ReaderBuilder::new();
    builder
        .delimiter(CLI.field_sep)
        .has_headers(false)
        .flexible(true)
        .escape(CLI.escape)
        .quote(CLI.quote as u8)
        .comment(CLI.comment);

    let mut schema = vec![];

    let mut rec_rdr = builder.from_reader(rdr);
    let mut line_count = 0;

    let mut header_field_count = 0;

    let sanity_sample = if CLI.headeron {
        CLI.sanity_sample + 1
    } else {
        CLI.sanity_sample
    };
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
                    schema.push(f);
                }
            }
            header_field_count = record.len();
        } else {
            if line_count > sanity_sample {
                break;
            }
            if !CLI.ignore_field_count || record.len() != header_field_count {
                return Err(anyhow!("Field count inconsistency: line: {}  field count: {}  expected field count: {}  file: {}", line_count, record.len(), header_field_count, &pathbuf.display()));
            }
        }

        if line_count > 10 {
            break;
        }
    }

    if CLI.headeron {
        if header_field_count == 0 {
            return Err(anyhow!(
                "Did not field headers so cannot schema-check on file: {}",
                pathbuf.display()
            ));
        }
        return Ok(schema);
    } else {
        if header_field_count == 0 {
            return Err(anyhow!(
                "Did not find anything (empty?) to schema-check on file: {}",
                pathbuf.display()
            ));
        } else {
            for i in 0..header_field_count {
                schema.push(Field {
                    pos: i as u32,
                    name: format!("f{}", i),
                    db_type: "text".to_string(),
                });
            }
        }
    }

    Ok(schema)
}



struct StringRecordParamed {
    raw_rec: StringRecord,
}

impl StringRecordParamed {
    pub fn new(raw_rec: StringRecord) -> StringRecordParamed {
        StringRecordParamed {
            raw_rec,
        }
    }
}

// impl rusqlite::ToSql for StringRecordsIntoIter<&str> {
//     fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
//         todo!()
//     }
// }



fn write_to_db(
    conn: &Connection,
    pathbuf: &PathBuf,
    tablename: &str,
    f_sch: &Vec<Field>,
) -> Result<(u64, u64)> {
    let mut rdr = match DecompressionReader::new(&pathbuf) {
        Ok(rdr) => rdr,
        Err(err) => Err(anyhow!(
            "Cannot read file \"{}\", due to error: {}",
            pathbuf.display(),
            err
        ))?,
    };
    let mut builder = csv::ReaderBuilder::new();
    builder
        .delimiter(CLI.field_sep)
        .has_headers(false)
        .flexible(true)
        .escape(CLI.escape)
        .quote(CLI.quote as u8)
        .comment(CLI.comment);
    let mut rec_rdr = builder.from_reader(rdr);
    let mut line_count = 0;
    let mut sql = format!(
        "insert into {} ( {} ) \nvalues( {} );",
        &tablename,
        f_sch
            .iter()
            .map(|f| format!("[{}]", &f.name))
            .collect::<Vec<String>>()
            .join(", "),
        f_sch
            .iter()
            .enumerate()
            .map(|(i, e)| format!("?{}", i + 1))
            .collect::<Vec<String>>()
            .join(", ")
    );

    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("Sql used: {}", &sql))?;
    info!("SQL for load: {}", &sql);
    conn.execute_batch("begin transaction;")?;

    let mut x_complete = std::cell::Cell::new(false);
    use scopeguard::defer;

    defer! {{
        if !x_complete.get() {
            error!("rollback in defer for write_to_db");
            if let Err(e) = conn.execute_batch("rollback;") {
                error!("There was a problem with deferal rollback: {}", e);
            }
        }
    }};


    let (mut row_count, mut field_count) = (0u64, 0u64);
    let mut record = StringRecord::new();
    let mut param_vec:Vec<String> = Vec::with_capacity(f_sch.len());
    while rec_rdr.read_record(&mut record)? {
        line_count += 1;
        if line_count == 1 && CLI.headeron {
            // skip this line and assume it was already checked header vs schema
        } else {
            // we know that stmt must be set by now
            // extend any missing blanks
            if !CLI.ignore_field_count {
                if record.len() != f_sch.len() {
                    return Err(anyhow!("Error trying batch insert record {}:{} field expected: {}  fields found: {}", pathbuf.display(),line_count, f_sch.len(), record.len()));
                }
            }
             
            param_vec.clear();
            for (i, s) in param_vec.iter_mut().enumerate() {
                s.clear();
                if i < record.len()  {
                    s.push_str(&record[i]);
                }
            }
            record.iter().for_each(|s| param_vec.push(s.to_string()));
            stmt.execute(rusqlite::params_from_iter(&param_vec))?;
            
            row_count += 1;
            field_count += f_sch.len() as u64;
        }
        // TODO:
    }
    conn.execute_batch("commit;")?;
    x_complete.set(true);

    Ok((row_count, field_count))
}

fn create_table(conn: &Connection, tablename: &str, f_sch: &Vec<Field>) -> Result<()> {
    let mut sql = format!("create table {} (\n", tablename);
    for f in f_sch.iter().take(f_sch.len() - 1) {
        sql.push_str(&format!("\t[{}] {},", f.name, f.db_type));
    }
    let last: &Field = f_sch.iter().rev().nth(0).unwrap();
    sql.push_str(&format!("\t[{}] {}\n);", last.name, last.db_type));
    info!("Executing create sql: {}", &sql);

    conn.execute(sql.as_str(), NO_PARAMS)?;

    Ok(())
}
