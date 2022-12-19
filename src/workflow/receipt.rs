use crate::db::{schema, schema::receipts};
use anyhow::anyhow;
use diesel::{Insertable, Queryable, RunQueryDsl, SqliteConnection};

#[derive(Queryable, Debug)]
pub struct Receipt {
    pub id: String,
    pub closure_cid: String, // FIXME Cid,
    pub val: i32,            // FIXME Ipld,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = receipts)]
pub struct NewReceipt {
    pub closure_cid: String, // FIXME Cid,
    pub val: i32,            //FIXME Ipld,
}

impl NewReceipt {
    pub fn insert(self: &Self, conn: &mut SqliteConnection) -> Result<usize, anyhow::Error> {
        diesel::insert_into(schema::receipts::table)
            .values(self)
            .execute(conn)
            .or_else(|_| Err(anyhow!("Unable to insert NewReceipt")))
    }
}
