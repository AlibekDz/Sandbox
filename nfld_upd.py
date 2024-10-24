from sqlalchemy import create_engine, Table, MetaData, update, case

# Establish connection to the PostgreSQL database
engine = create_engine('postgresql://user:password@localhost:5432/mydatabase')

# Metadata object to load table information
metadata = MetaData(bind=engine)

# Load the 'nfld' and 'ld' tables from the database schema
nfld = Table('nfld', metadata, autoload_with=engine)
ld = Table('ld', metadata, autoload_with=engine)

# Open a session with the database
with engine.connect() as conn:
    # Create the update statement for the 'nfld' table
    stmt = (
        update(nfld)
        .values(
            # Update 'd1_nfld_arc' based on conditions from 'ld' table
            d1_nfld_arc=case(
                [(ld.c.d1_ld_smin > ld.c.d1_ld_med, '1D LD UH')], else_='1D LD LH'
            ),
            # Update 'h3_nfld_arc' based on conditions from 'ld' table
            h3_nfld_arc=case(
                [(ld.c.h3_ld_smin > ld.c.h3_ld_med, '3H LD UH')], else_='3H LD LH'
            ),
            # Update 'm30_nfld_arc' based on conditions from 'ld' table
            m30_nfld_arc=case(
                [(ld.c.m30_ld_smin > ld.c.m30_ld_med, '30M LD UH')], else_='30M LD LH'
            ),
            # Update 'd1_nfld_rsx' based on conditions from 'ld' table
            d1_nfld_rsx=case(
                [(ld.c.d1_ld_rsx > ld.c.d1_ld_rsxsig, '1D RSXUP')], else_='1D RSXDN'
            ),
            # Update 'h3_nfld_rsx' based on conditions from 'ld' table
            h3_nfld_rsx=case(
                [(ld.c.h3_ld_rsx > ld.c.h3_ld_rsxsig, '3H RSXUP')], else_='3H RSXDN'
            ),
            # Update 'm30_nfld_rsx' based on conditions from 'ld' table
            m30_nfld_rsx=case(
                [(ld.c.m30_ld_rsx > ld.c.m30_ld_rsxsig, '30M RSXUP')], else_='30M RSXDN'
            ),
            # Update 'd1_nfld_log' based on volume alpha condition
            d1_nfld_log=case(
                [(ld.c.d1_ld_volalpha > ld.c.d1_ld_vmin, '1D LDUP')], else_='1D LDDN'
            ),
            # Update 'h3_nfld_log' based on volume alpha condition
            h3_nfld_log=case(
                [(ld.c.h3_ld_volalpha > ld.c.h3_ld_vmin, '3H LDUP')], else_='3H LDDN'
            ),
            # Update 'm30_nfld_log' based on volume alpha condition
            m30_nfld_log=case(
                [(ld.c.m30_ld_volalpha > ld.c.m30_ld_vmin, '30M LDUP')], else_='30M LDDN'
            ),
            # Update 'd1_nfld_vola' based on minimum conditions
            d1_nfld_vola=case(
                [(ld.c.d1_ld_smin > ld.c.d1_ld_vmin, '1D VOLA UP')], else_='1D VOLA DN'
            ),
            # Update 'h3_nfld_vola' based on minimum conditions
            h3_nfld_vola=case(
                [(ld.c.h3_ld_smin > ld.c.h3_ld_vmin, '3H VOLA UP')], else_='3H VOLA DN'
            ),
            # Update 'm30_nfld_vola' based on minimum conditions
            m30_nfld_vola=case(
                [(ld.c.m30_ld_smin > ld.c.m30_ld_vmin, '30M VOLA UP')], else_='30M VOLA DN'
            ),
            # Update 'd1_nfld_rband' based on minimum conditions
            d1_nfld_rband=case(
                [(ld.c.d1_ld_rmin > ld.c.d1_ld_rmin2, '1D RBUP')], else_='1D RBDN'
            ),
            # Update 'h3_nfld_rband' based on minimum conditions
            h3_nfld_rband=case(
                [(ld.c.h3_ld_rmin > ld.c.h3_ld_rmin2, '3H RBUP')], else_='3H RBDN'
            ),
            # Update 'm30_nfld_rband' based on minimum conditions
            m30_nfld_rband=case(
                [(ld.c.m30_ld_rmin > ld.c.m30_ld_rmin2, '30M RBUP')], else_='30M RBDN'
            ),
            # Update 'd1_nfld_vol' based on volume conditions
            d1_nfld_vol=case(
                [(ld.c.d1_ld_vol > 0, '1DV UP')], else_='1DV DN'
            ),
            # Update 'h3_nfld_vol' based on volume conditions
            h3_nfld_vol=case(
                [(ld.c.h3_ld_vol > 0, '3HV UP')], else_='3HV DN'
            ),
            # Update 'm30_nfld_vol' based on volume conditions
            m30_nfld_vol=case(
                [(ld.c.m30_ld_vol > 0, '30M UP')], else_='30M VDN'
            )
        )
        .where(nfld.c.nfld_id == ld.c.ld_id)  # Match records based on ID
    )
    
    # Execute the update statement
    conn.execute(stmt)
