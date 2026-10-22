/* ===========================================================================
   Tier 1 Insurance Path workshop - FACILITATOR SETUP
   Implements setup/PERMISSIONS.md. Read that file first if a customer admin is
   running this for you.

   Design: one role per attendee, and that role OWNS the attendee's schema.
   Owning the schema means the attendee role owns RAW, the Dynamic Tables, the
   dbt project object, and every table dbt materializes. That is what keeps
   Dynamic Table refreshes and change-tracking enablement working (both run as
   or require the owner of RAW). Do not create a shared, facilitator-owned RAW.

   No ACCOUNTADMIN is used. SYSADMIN creates the database and warehouse;
   USERADMIN creates roles and users. Swap in your account's equivalents.

   You do NOT need reference/legacy_pipeline_setup.sql. It is an optional prop
   for section 3.1; skip it on a first delivery.
   =========================================================================== */


/* --- 1. Shared objects (SYSADMIN) ---------------------------------------- */

USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS NORTHPOINT_LAB
    COMMENT = 'Tier 1 Insurance Path DE workshop. Event JSON to Snowpipe Streaming, Silver via DT or dbt.';

CREATE WAREHOUSE IF NOT EXISTS NORTHPOINT_LAB_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Workshop warehouse: queries, DT refresh, dbt runs';


/* --- 2. Per-attendee role and schema ------------------------------------
   Repeat this block per attendee. Replace 01 and <attendee_user>.

   The role is granted up to SYSADMIN so the facilitator can see and tear down
   everything an attendee builds.
   ------------------------------------------------------------------------ */

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS LAB_ATTENDEE_01 COMMENT = 'Insurance-path lab attendee 01';
GRANT ROLE LAB_ATTENDEE_01 TO ROLE SYSADMIN;
-- GRANT ROLE LAB_ATTENDEE_01 TO USER <attendee_user>;
-- ALTER USER <attendee_user> SET DEFAULT_ROLE = LAB_ATTENDEE_01 DEFAULT_WAREHOUSE = NORTHPOINT_LAB_WH;

USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS NORTHPOINT_LAB.ATTENDEE_01
    COMMENT = 'Insurance-path lab workspace for attendee 01';

-- PERMISSIONS.md rows 1 to 3.
GRANT USAGE ON DATABASE  NORTHPOINT_LAB    TO ROLE LAB_ATTENDEE_01;
GRANT USAGE ON WAREHOUSE NORTHPOINT_LAB_WH TO ROLE LAB_ATTENDEE_01;
GRANT OWNERSHIP ON SCHEMA NORTHPOINT_LAB.ATTENDEE_01 TO ROLE LAB_ATTENDEE_01 COPY CURRENT GRANTS;

/* If the customer will not grant schema OWNERSHIP, use these instead of the
   OWNERSHIP line above (PERMISSIONS.md rows 4 to 7, plus the future grants it
   names). The attendee still creates, and therefore owns, RAW itself.

GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE, CREATE DBT PROJECT
    ON SCHEMA NORTHPOINT_LAB.ATTENDEE_01 TO ROLE LAB_ATTENDEE_01;
GRANT OPERATE, MONITOR ON FUTURE DYNAMIC TABLES IN SCHEMA NORTHPOINT_LAB.ATTENDEE_01 TO ROLE LAB_ATTENDEE_01;
GRANT USAGE, MONITOR   ON FUTURE DBT PROJECTS   IN SCHEMA NORTHPOINT_LAB.ATTENDEE_01 TO ROLE LAB_ATTENDEE_01;
*/


/* --- 3. Personal access tokens ------------------------------------------
   The token secret is returned ONCE, by this statement. Capture it now and
   send it to the attendee privately. Restrict it to the attendee role, so the
   producer cannot run as anything broader.
   ------------------------------------------------------------------------ */

-- ALTER USER <attendee_user> ADD PROGRAMMATIC ACCESS TOKEN NORTHPOINT_LAB
--     ROLE_RESTRICTION = 'LAB_ATTENDEE_01'
--     DAYS_TO_EXPIRY = 2
--     COMMENT = 'Insurance-path workshop. Revoke after the session.';

/* Key-pair fallback, if account policy forbids PATs.
   Attendee runs:
     openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
     openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   Facilitator runs, per attendee:
     ALTER USER <attendee_user> SET RSA_PUBLIC_KEY='<rsa_key.pub, headers stripped>';
   The producer then uses authorization_type "JWT" with private_key_file.
   Generate keys before the session; budget +10 min in section 1 otherwise.
*/


/* --- 4. Path B pre-flight: Workspaces -----------------------------------
   dbt Projects on Snowflake are authored in a Workspace, which lives in the
   user's personal database (USER$<name>.PUBLIC). Have each attendee sign in
   to Snowsight once before the session and confirm Projects > Workspaces
   lets them create a workspace. Some accounts restrict personal databases;
   find out before the room does.

   Also confirm the dbt project object version behavior on this account. The
   lab assumes the single mutable live version (2026_06 bundle). Check with:
   ------------------------------------------------------------------------ */

SELECT SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2026_06') AS bundle_2026_06_status;


/* --- 5. The account identifier attendees will need ----------------------
   The SDK URL needs the ORG-QUALIFIED identifier, not the account locator.
   It is the single most common setup failure.
   ------------------------------------------------------------------------ */

SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS SNOWFLAKE_ACCOUNT_FOR_SDK,
       CURRENT_ACCOUNT()                                            AS account_locator_DO_NOT_USE;


/* --- 6. Confirm ---------------------------------------------------------- */

SHOW SCHEMAS IN DATABASE NORTHPOINT_LAB;
SHOW GRANTS TO ROLE LAB_ATTENDEE_01;
