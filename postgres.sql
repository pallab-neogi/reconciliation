
drop table public.match_trans;
drop table public.unmatch_trans_markoffacc;
drop table public.unmatch_trans_markoffrej;
drop table public.unmatch_trans_upf_casa_card


CREATE TABLE match_trans (
    id varchar(50) PRIMARY KEY,
    doctype VARCHAR(20),
	transdatetime TIMESTAMP,
    uetr VARCHAR(50)
);

CREATE TABLE match_trans_recon2 (
    id varchar(50) PRIMARY KEY,
    doctype VARCHAR(20),
	transdatetime TIMESTAMP,
    uetr VARCHAR(50)
);

CREATE TABLE match_trans_recon3 (
    id varchar(50) PRIMARY KEY,
    doctype VARCHAR(20),
	transdatetime TIMESTAMP,
    uetr VARCHAR(50)
);

CREATE TABLE unmatch_trans_UPF_CASA_CARD (
    id varchar(50) PRIMARY KEY,
    doctype VARCHAR(20),
    recordtype VARCHAR(20),
    accountnumber VARCHAR(50), 
    todaysdate VARCHAR(50),
    amount VARCHAR(50),
    entrysign VARCHAR(50),
    transdatetime TIMESTAMP,
    ref1 VARCHAR(50),
    uetr VARCHAR(50),
    ref2 VARCHAR(50),
    ref4 VARCHAR(50),
    status VARCHAR(50),
    reason VARCHAR(50),
    reconcile bool,
    crtdate VARCHAR(50),
    upddt VARCHAR(50)
);

CREATE TABLE unmatch_trans_markoffacc_recon1(
    id varchar(50) PRIMARY KEY,
    doctype VARCHAR(20),
    product VARCHAR(20),
    scheme VARCHAR(50), 
    msgid VARCHAR(50),
    endtoendid VARCHAR(50),
    uetr VARCHAR(50),
    instructbic VARCHAR(50),
    creditoragentbic VARCHAR(50),
    amount VARCHAR(50),
    currency VARCHAR(50),
    debtorname VARCHAR(50),
    creditorname VARCHAR(50),
    settlementdate VARCHAR(50),
    transdatetime TIMESTAMP,
    settlementpoolref VARCHAR(50),
    reasondesc VARCHAR(50),
    entrysign VARCHAR(50),
    reconcile bool,
    status VARCHAR(50),
    crtdate VARCHAR(50),
    upddt VARCHAR(50)
);

CREATE TABLE unmatch_trans_markoffrej(
    id varchar(50) PRIMARY KEY,
    doctype VARCHAR(20),
    product VARCHAR(20),
    scheme VARCHAR(50), 
    msgid VARCHAR(50),
    endtoendid VARCHAR(50),
    uetr VARCHAR(50),
    instructbic VARCHAR(50),
    payeeagentbic VARCHAR(50),
    amount VARCHAR(50),
    currency VARCHAR(50),
    category VARCHAR(50),
    status VARCHAR(50),
    reasoncode VARCHAR(50),
    reasondesc VARCHAR(50),
    transdatetime TIMESTAMP,
    entrysign VARCHAR(50),
    reconcile bool,
    crtdate VARCHAR(50),
    upddt VARCHAR(50)
);

CREATE TABLE unmatch_trans_GL_recon2(
	id varchar(50) PRIMARY KEY,
	doctype VARCHAR(20),
	recon VARCHAR(20),
	recordtype VARCHAR(20),
	account VARCHAR(50),
	currency VARCHAR(50),
	transdatetime TIMESTAMP,
	amount decimal(20,4),
	entrysign VARCHAR(50),
	Date1 VARCHAR(50),
	Date2 VARCHAR(50),
	uetr VARCHAR(50),
	branch VARCHAR(50),
	reconcile bool,
	crtdate VARCHAR(50),
	upddt VARCHAR(50)
)

CREATE TABLE unmatch_trans_GL_recon3(
	id varchar(50) PRIMARY KEY,
	doctype VARCHAR(20),
	recon VARCHAR(20),
	recordtype VARCHAR(20),
	account VARCHAR(50),
	currency VARCHAR(50),
	transdatetime TIMESTAMP,
	amount decimal(20,4),
	entrysign VARCHAR(50),
	Date1 VARCHAR(50),
	Date2 VARCHAR(50),
	uetr VARCHAR(50),
	branch VARCHAR(50),
	reconcile bool,
	crtdate VARCHAR(50),
	upddt VARCHAR(50)
)


CREATE TABLE unmatch_trans_markoffacc_recon2(
    id varchar(50) PRIMARY KEY,
    doctype VARCHAR(20),
    product VARCHAR(20),
    scheme VARCHAR(50), 
    msgid VARCHAR(50),
    endtoendid VARCHAR(50),
    uetr VARCHAR(50),
    uetr30 VARCHAR(50),
    instructbic VARCHAR(50),
    creditoragentbic VARCHAR(50),
    amount VARCHAR(50),
    currency VARCHAR(50),
    debtorname VARCHAR(50),
    creditorname VARCHAR(50),
    settlementdate VARCHAR(50),
    transdatetime TIMESTAMP,
    settlementpoolref VARCHAR(50),
    reasondesc VARCHAR(50),
    entrysign VARCHAR(50),
    reconcile bool,
    status VARCHAR(50),
    crtdate VARCHAR(50),
    upddt VARCHAR(50)
);

CREATE TABLE unmatch_trans_markoffacc_recon3(
    id varchar(50) PRIMARY KEY,
    doctype VARCHAR(20),
    product VARCHAR(20),
    scheme VARCHAR(50), 
    msgid VARCHAR(50),
    endtoendid VARCHAR(50),
    uetr VARCHAR(50),
    uetr30 VARCHAR(50),
    instructbic VARCHAR(50),
    creditoragentbic VARCHAR(50),
    amount VARCHAR(50),
    currency VARCHAR(50),
    debtorname VARCHAR(50),
    creditorname VARCHAR(50),
    settlementdate VARCHAR(50),
    transdatetime TIMESTAMP,
    settlementpoolref VARCHAR(50),
    reasondesc VARCHAR(50),
    entrysign VARCHAR(50),
    reconcile bool,
    status VARCHAR(50),
    crtdate VARCHAR(50),
    upddt VARCHAR(50)
);