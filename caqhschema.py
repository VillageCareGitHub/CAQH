import psycopg2
import configparser

# SCHEMA SELECTION
DBSCHEMA=''

# TEST INDICATOR boolean true or false
TESTIND=False

# AWS Credentials Information for Databases
initConfig = configparser.ConfigParser()
initConfig.read("AWS_List.config") 

# Database connection to redshift
conn=psycopg2.connect(dbname= 'dev', host=initConfig.get('profile prod', 'host'), 
port= initConfig.get('profile prod', 'port'), user= initConfig.get('profile prod', 'dbuser'), password= initConfig.get('profile prod', 'dbpwd'))

# Drop table if exist
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_providers"""
cur=conn.cursor()
cur.execute(SQL)
conn.commit()


# Create tables for Providers
SQL=f""" CREATE TABLE {DBSCHEMA}caqh_providers (
    caqh_id INTEGER not null,    
    provider_first_name NVARCHAR(255),
    provider_last_name NVARCHAR(255),
    provider_type NVARCHAR(50),
    psv_reserved NVARCHAR(255),
    psv_status NVARCHAR(50),
    requested_decision_date nvarchar(20),
    requested_state NVARCHAR(2),
    service_type NVARCHAR(100),
    target_decision_date nvarchar(255),
    actual_delivery_date nvarchar(255),
    submission_complete_date nvarchar(255),
    file_notes NVARCHAR(8000),
    last_attestation_timestamp nvarchar(255),
    po_provider_id NVARCHAR(255),
    primary_practice_state NVARCHAR(2),
    primary_specialty NVARCHAR(8000),
    fileuploaded nvarchar(800),
    uploaddate date,
    PRIMARY KEY(caqh_id)
) """


cur.execute(SQL)
conn.commit()

# CAQH_ATTESTATION Tables
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_attestation"""
cur=conn.cursor()
cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_attestation (
    caqh_attestation_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    last_attestation_date NVARCHAR(255),
    attestation_status NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
    )"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    # Testing SQL
    SQL=f"""
    INSERT INTO {DBSCHEMA}caqh_attestation (caqh_id,last_attestation_date,attestation_status,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag) 
    values ('12521344','2018-12-27','Re-Attestation','A','True','ProView Application','ProView','2019-01-17','Gunasekar Sivasubramanian','','DE100-1','')
    """
    cur.execute(SQL)
    conn.commit()

# CAQH_STATE_LICENSE Tables
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_state_license"""
cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_state_license (
    caqh_state_license_id int IDENTITY(1,1),
    caqh_id INTEGER,
    license_state NVARCHAR(255),
    license_number NVARCHAR(255),
    expiration_date NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    issue_date NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_state_license (caqh_id,license_state,license_number,expiration_date,category,verified,verification_method,verification_source,issue_date,verification_date,analyst_id,analyst_notes,category_id,flag) 
    values ('12521344','NY','048086','2020-05-31','A','True','Manual Web Query','State Licensing Board','2004-09-01','2019-01-17','Gunasekar Sivasubramanian','','DE110-1','')
    """

    cur.execute(SQL)
    conn.commit()

# CAQH_EDUCATION Tables
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_education"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_education (
    caqh_education_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    degree_name NVARCHAR(255),
    institution NVARCHAR(255),
    graduation_year NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_education (caqh_id,degree_name,institution,graduation_year,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag) 
    values ('12521344','CSW','Not Available','','A','True','State Licensing Board Verification','State Licensing Board','2019-01-17','Gunasekar Sivasubramanian','','DE160-1','')
    """

    cur.execute(SQL)
    conn.commit()


# CAQH_DISCLOSURE TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_disclosure"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_disclosure (
    caqh_disclosure_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    non_preferred_answers NVARCHAR(255),
    disclosure_summary NVARCHAR(8000),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_disclosure (caqh_id,non_preferred_answers,disclosure_summary,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag) 
    values ('12521344','False','[]','A','True','ProView Application','ProView','2019-01-17','Gunasekar Sivasubramanian','','DE170-1','')
    """

    cur.execute(SQL)
    conn.commit()

# CAQH_WORK_HISTORY TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_work_history"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_work_history (
    caqh_work_history_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    six_month_gaps NVARCHAR(255),
    valid_explanations NVARCHAR(255),
    gaps NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_work_history (caqh_id,six_month_gaps,valid_explanations,gaps,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('12521344','False','False','[]','A','True','ProView Application','ProView','2019-01-17','Gunasekar Sivasubramanian','','DE180-1','')
    """

    cur.execute(SQL)
    conn.commit()


# CAQH_PLI TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_pli"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_pli (
    caqh_pli_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    carrier_name NVARCHAR(255),
    unlimited_coverage NVARCHAR(255),
    occurrence_coverage NVARCHAR(255),
    aggregate_coverage NVARCHAR(255),
    coverage_effective_date NVARCHAR(255),
    coverage_end_date NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_pli (caqh_id,carrier_name,unlimited_coverage,occurrence_coverage,aggregate_coverage,coverage_effective_date,coverage_end_date,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('12521344','NETWORK ADVANTAGE INSURANCE LTD.','False','2000000','5000000','2018-01-01','2019-01-01','Q','True','ProView Attachment','ProView','2019-01-17','Gunasekar Sivasubramanian','Unable to obtain evidence of coverage (Policy is expired or expires before Target Delivery Date)','DE190-11','Expired')
    """

    cur.execute(SQL)
    conn.commit()


# CAQH MALPRACTICE HISTORY TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_malpractice_history"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_malpractice_history (
    caqh_malpractice_history_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    malpractice_history NVARCHAR(255),
    malpractice_date NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_malpractice_history (caqh_id,malpractice_history,malpractice_date,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('12521344','False','[]','A','True','Automated Web Query','NPDB','2019-01-07','Gunasekar Sivasubramanian','','DE210-1','')
    """

    cur.execute(SQL)
    conn.commit()

# CAQH DISCIPLINARY SANCTIONS TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_disciplinary_sanctions"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_disciplinary_sanctions (
    caqh_disciplinary_sanctions_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    sanctions_history NVARCHAR(255),
    sanction_date NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_disciplinary_sanctions (caqh_id,sanctions_history,sanction_date,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('12521344','False','[]','A','True','Automated Web Query','NPDB','2019-01-07','Gunasekar Sivasubramanian','','DE220-1','','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()

# CAQH MEDICARE MEDICAID SANCTIONS TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_medicare_medicaid_sanctions"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_medicare_medicaid_sanctions (
    caqh_medicare_medicaid_sanctions_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    medicare_medicaid_sanctions NVARCHAR(255),
    sanction_date NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_medicare_medicaid_sanctions (caqh_id,medicare_medicaid_sanctions,sanction_date,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('12521344','False','[]','A','True','Automated Web Query','NPDB','2019-01-07','Gunasekar Sivasubramanian','','DE230-1','','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()


# CAQH OIG TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_oig"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_oig (
    caqh_oig_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    oig_listed NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_oig (caqh_id,oig_listed,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('12521344','False','A','True','Offline Database,  date last updated:2018-12-10','OIG','2019-01-07','Gunasekar Sivasubramanian','','DE240-1','','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()


# CAQH SAM TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_sam"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_sam (
    caqh_sam_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    sam_listed NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_sam (caqh_id,sam_listed,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('12521344','False','A','True','Offline Database,  date last updated:2018-11-18','SAM','2019-01-07','Gunasekar Sivasubramanian','','DE250-1','','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()

# CAQH MEDICARE OPT OUT TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_medicare_opt_out"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_medicare_opt_out (
    caqh_medicare_opt_out_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    state NVARCHAR(255),
    medicare_opt_out_listed NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_medicare_opt_out (caqh_id,state,medicare_opt_out_listed,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('12521344','NY','False','A','True','Manual Web Query','State Specific MAC Organization','2019-01-17','Gunasekar Sivasubramanian','','DE260-1','','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()

# CAQH DEA TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_dea"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_dea (
    caqh_dea_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    dea_state NVARCHAR(255),
    dea_number NVARCHAR(255),
    expiration_date NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_dea (caqh_id,dea_state,dea_number,expiration_date,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('11318216','NY','BH8556904','2021-10-31','A','True','Manual Web Query','NTIS','2019-01-11','Gunasekar Sivasubramanian','','DE120-1','','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()

# CAQH TRAINING TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_training"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_training (
    caqh_training_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    training_type NVARCHAR(255),
    institution NVARCHAR(255),
    department NVARCHAR(255),
    specialty NVARCHAR(255),
    training_accredited NVARCHAR(255),
    start_date NVARCHAR(255),
    completion_date NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_training (caqh_id,training_type,institution,department,specialty,training_accredited,start_date,completion_date,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('11318216','Residency','Temple University Hospital','Foundation Associate of The Professional Wound Care Association','Podiatrist','false','2000-06-01','2016-12-08','Q','True','FAX','Training Program','2019-01-15','Gunasekar Sivasubramanian','Unable to obtain/verify','DE150-12','','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()



# CAQH BOARD EDUCATION TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_board_certification"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_board_certification (
    caqh_board_certification_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    certifying_board NVARCHAR(255),
    specialty NVARCHAR(255),
    certification_date NVARCHAR(255),
    reverification_date NVARCHAR(255),
    expiration_date NVARCHAR(255),
    lifetime_cert NVARCHAR(255),
    moc_req_met NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_board_certification (caqh_id,certifying_board,specialty,certification_date,reverification_date,expiration_date,lifetime_cert,moc_req_met,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('13998837','American Academy of Nurse Practitioners','Nurse Practitioner, Family','2017-02-23','','','','','','False','','','','','','','Not Evaluated','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()


# CAQH HOSPITAL PRIVILEGES TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_hospital_privileges"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_hospital_privileges (
    caqh_hospital_privileges_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    has_privileges NVARCHAR(255),
    privilege_type NVARCHAR(255),
    hospital_name NVARCHAR(255),
    address NVARCHAR(255),
    address_2 NVARCHAR(255),
    city NVARCHAR(255),
    state NVARCHAR(255),
    zip NVARCHAR(255),
    privilege_status NVARCHAR(255),
    temporary_privilege NVARCHAR(255),
    expiration_date NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_hospital_privileges (caqh_id,has_privileges,privilege_type,hospital_name,address,address_2,city,state,zip,privilege_status,temporary_privilege,expiration_date,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('11209095','False','Arrangement','','','','','','','','','','','False','','','','','','','','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()

# CAQH CDS TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_cds"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_cds (
    caqh_cds_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    cds_state NVARCHAR(255),
    cds_number NVARCHAR(255),
    expiration_date NVARCHAR(255),
    category NVARCHAR(255),
    verified NVARCHAR(255),
    verification_method NVARCHAR(255),
    verification_source NVARCHAR(255),
    verification_date NVARCHAR(255),
    analyst_id NVARCHAR(255),
    analyst_notes NVARCHAR(8000),
    category_id NVARCHAR(255),
    flag NVARCHAR(255),
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_cds (caqh_id,cds_state,cds_number,expiration_date,category,verified,verification_method,verification_source,verification_date,analyst_id,analyst_notes,category_id,flag)
    values ('10831670','IN','01077842B','2019-10-31','','False','','','','','','','','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()

# CAQH CATEGORY COUNT TABLE
SQL=f""" DROP TABLE IF EXISTS {DBSCHEMA}caqh_category_counts"""

cur.execute(SQL)
conn.commit()

SQL=f""" CREATE TABLE {DBSCHEMA}caqh_category_counts (
    caqh_category_counts_id INTEGER IDENTITY(1,1),
    caqh_id INTEGER,
    A INTEGER,
    B INTEGER,
    C INTEGER,
    D INTEGER,
    Q INTEGER,
    fileuploaded nvarchar(800),
    uploaddate date
)"""

cur.execute(SQL)
conn.commit()

if TESTIND==True:
    #TEST
    SQL=f"""INSERT INTO {DBSCHEMA}caqh_category_counts (caqh_id,A,B,C,D,Q)
    values ('10831670','7','0','0','0','1','testfile.json','02/18/2019')
    """

    cur.execute(SQL)
    conn.commit()


conn.close()