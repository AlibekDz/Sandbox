from sqlalchemy import create_engine, Table, MetaData, update, case

# Establish connection to the PostgreSQL database
engine = create_engine('postgresql://user:password@localhost:5432/mydatabase')

# Metadata object to load table information
metadata = MetaData(bind=engine)

# Load the 'cem' and 'r_table' tables from the database schema
cem = Table('cem', metadata, autoload_with=engine)
r_table = Table('r_table', metadata, autoload_with=engine)

# Open a session with the database
with engine.connect() as conn:
    # Create the update statement for the 'cem' table
    stmt = (
        update(cem)
        .values(
            # Update 'd1_cem_arc' based on the condition from 'r_table'
            d1_cem_arc=case(
                [(r_table.c.d1_r_smin > r_table.c.d1_r_med, '1D UH')], else_='1D LH'
            ),
            # Update 'h3_cem_arc' based on the condition from 'r_table'
            h3_cem_arc=case(
                [(r_table.c.h3_r_smin > r_table.c.h3_r_med, '3H UH')], else_='3H LH'
            ),
            # Update 'm30_cem_arc' based on the condition from 'r_table'
            m30_cem_arc=case(
                [(r_table.c.m30_r_smin > r_table.c.m30_r_med, '30M UH')], else_='30M LH'
            ),
            # Update 'd1_h3_ccem_q' based on several conditions from 'r_table'
            d1_h3_ccem_q=case(
                [
                    (r_table.c.h3_r_med > r_table.c.d1_r_med 
                     & r_table.c.h3_r_smin > r_table.c.h3_r_med 
                     & r_table.c.d1_r_smin > r_table.c.h3_r_med, 'QUR'),
                    (r_table.c.h3_r_med > r_table.c.d1_r_med 
                     & (r_table.c.h3_r_smin < r_table.c.h3_r_med 
                        | r_table.c.d1_r_smin < r_table.c.h3_r_med), 'QUD'),
                    (r_table.c.h3_r_med < r_table.c.d1_r_med 
                     & r_table.c.h3_r_smin < r_table.c.h3_r_med 
                     & r_table.c.d1_r_smin < r_table.c.h3_r_med, 'QLD'),
                    (r_table.c.h3_r_med < r_table.c.d1_r_med 
                     & (r_table.c.h3_r_smin > r_table.c.h3_r_med 
                        | r_table.c.d1_r_smin > r_table.c.h3_r_med), 'QLR')
                ], 
                else_=cem.c.d1_h3_ccem_q
            ),
            # Update 'h3_m30_ccem_q' with similar logic for different columns
            h3_m30_ccem_q=case(
                [
                    (r_table.c.m30_r_med > r_table.c.h3_r_med 
                     & r_table.c.m30_r_smin > r_table.c.m30_r_med 
                     & r_table.c.h3_r_smin > r_table.c.m30_r_med, 'QUR'),
                    (r_table.c.m30_r_med > r_table.c.h3_r_med 
                     & (r_table.c.m30_r_smin < r_table.c.m30_r_med 
                        | r_table.c.h3_r_smin < r_table.c.m30_r_med), 'QUD'),
                    (r_table.c.m30_r_med < r_table.c.h3_r_med 
                     & r_table.c.m30_r_smin < r_table.c.m30_r_med 
                     & r_table.c.h3_r_smin < r_table.c.m30_r_med, 'QLD'),
                    (r_table.c.m30_r_med < r_table.c.h3_r_med 
                     & (r_table.c.m30_r_smin > r_table.c.m30_r_med 
                        | r_table.c.h3_r_smin > r_table.c.m30_r_med), 'QLR')
                ], 
                else_=cem.c.h3_m30_ccem_q
            ),
            # Update 'd1_cem_sr' based on previous values and new conditions
            d1_cem_sr=case(
                [
                    (cem.c.d1_cem_sr == 'UH' & r_table.c.d1_r_rmin > r_table.c.d1_r_smin, '1D TR'),
                    (cem.c.d1_cem_sr == 'LH' & r_table.c.d1_r_rmin > r_table.c.d1_r_smin, '1D PB'),
                    (cem.c.d1_cem_sr == 'UH', '1D PB')
                ], 
                else_='1D TR'
            ),
            # Similar updates for 'h3_cem_sr', 'm30_cem_sr'
            h3_cem_sr=case(
                [
                    ((cem.c.d1_h3_ccem_q == 'QUR' | cem.c.d1_h3_ccem_q == 'QLR') 
                     & r_table.c.h3_r_rmin > r_table.c.h3_r_smin, '3H TR'),
                    ((cem.c.d1_h3_ccem_q == 'QUD' | cem.c.d1_h3_ccem_q == 'QLD') 
                     & r_table.c.h3_r_rmin > r_table.c.h3_r_smin, '3H PB'),
                    ((cem.c.d1_h3_ccem_q == 'QUD' | cem.c.d1_h3_ccem_q == 'QLD'), '3H TR')
                ], 
                else_='3H PB'
            ),
            m30_cem_sr=case(
                [
                    ((cem.c.h3_m30_ccem_q == 'QUR' | cem.c.h3_m30_ccem_q == 'QLR') 
                     & r_table.c.m30_r_rmin > r_table.c.m30_r_smin, '30M TR'),
                    ((cem.c.h3_m30_ccem_q == 'QUD' | cem.c.h3_m30_ccem_q == 'QLD') 
                     & r_table.c.m30_r_rmin > r_table.c.m30_r_smin, '30M PB'),
                    ((cem.c.h3_m30_ccem_q == 'QUD' | cem.c.h3_m30_ccem_q == 'QLD'), '30M TR')
                ], 
                else_='30M PB'
            )
        )
        .where(cem.c.cem_id == r_table.c.r_id)  # Matching the records by ID
    )
    
    # Execute the update statement
    conn.execute(stmt)
