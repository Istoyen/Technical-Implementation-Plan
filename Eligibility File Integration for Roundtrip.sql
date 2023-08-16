WITH
  alldata AS (
    SELECT DISTINCT
      "Plan" || "PBP" AS "Plan ID",
      CASE
        WHEN LEFT("Product ID", 3) = 'HMO' THEN 'Mass Advantage Basic (HMO)'
        WHEN LEFT("Product ID", 3) = 'Plus HMO' THEN 'Mass Advantage Plus (HMO)'
        WHEN LEFT("Product ID", 3) = 'PPO' THEN 'Mass Advantage Premiere (PPO)'
      END AS "Plan Name",
      "Member ID" AS "Member",
      "First Name" AS "First Name",
      "Last Name" AS "Last Name",
      TRIM("Email") AS "Email Address",
      '(' || (
        SUBSTRING(REPLACE("Home_Phone_Nbr", '-', ''), 1, 3) || ')' || SUBSTRING(REPLACE("Home_Phone_Nbr", '-', ''), 4, 3) || '-' || SUBSTRING(REPLACE("Home_Phone_Nbr", '-', ''), 7, 4)
      ) AS "Phone (Mobile Preferred)",
      "DOB" AS "Date of Birth",
      "Gender" AS Gender,
      "Mem Address Line1" AS Address,
      "Mem City" AS City,
      "Mem State Code" AS State,
      "Mem Zip Code" AS Zip,
      'Eastern Time(US and Canada)' AS "Time Zone",
      ' ' AS Budget,
      CASE
        WHEN LEFT("Product ID", 3) = 'HMO' THEN '12'
        WHEN LEFT("Product ID", 3) = 'Plus HMO' THEN '12'
        WHEN LEFT("Product ID", 3) = 'PPO' THEN '6'
      END AS "Rides Limit",
      CASE
        WHEN "Language" = 'SPANISH' THEN 'ES'
        ELSE 'EN'
      END AS "Language Preference",
      ' ' AS "Communication Preference",
      --TO_DATE("Enroll Eff Start Date"::VARCHAR, 'MM/DD/YYYY') AS "Effective Date",
      CAST("Enroll Eff Start Date" AS date) AS "Effective Date",
      CASE
        WHEN "Enroll Eff End Date" = '12/31/9999' THEN NULL
        ELSE "Enroll Eff End Date"
      END AS "Expiration Date"
    FROM
      "Wipro Membership Ingest"
  ),
  currentyeardata AS (
    SELECT
      "Plan ID" || '|' AS "Plan ID",
      "Plan Name" || '|' AS "Plan Name",
      "Member" || '|' AS "Member",
      "First Name" || '|' AS "First Name",
      "Last Name" || '|' AS "Last Name",
      "Email Address" || '|' AS "Email Address",
      "Phone (Mobile Preferred)" || '|' AS "Phone (Mobile Preferred)",
      "Date of Birth" || '|' AS "Date of Birth",
      Gender || '|' AS Gender,
      Address || '|' AS Address,
      City || '|' AS City,
      State || '|' AS State,
      Zip || '|' AS Zip,
      "Time Zone" || '|' AS "Time Zone",
      Budget || '|' AS Budget,
      "Rides Limit" || '|' AS "Rides Limit",
      "Language Preference" || '|' AS "Language Preference",
      "Communication Preference" || '|' AS "Communication Preference",
      TO_CHAR("Effective Date", 'MM/DD/YYYY') || '|' AS "Effective Date",
      COALESCE("Expiration Date", '') AS "Expiration Date",
      ROW_NUMBER() OVER (
        PARTITION BY
          "Member"
        ORDER BY
          "Expiration Date" ASC
      ) || E'\r\n' AS relevant
    FROM
      alldata
    WHERE
      DATE_PART('year', "Effective Date")::INTEGER <= DATE_PART('year', CURRENT_DATE)::INTEGER
  ),
  nextyeardata AS (
    SELECT
      "Plan ID" || '|' AS "Plan ID",
      "Plan Name" || '|' AS "Plan Name",
      "Member" || '|' AS "Member",
      "First Name" || '|' AS "First Name",
      "Last Name" || '|' AS "Last Name",
      "Email Address" || '|' AS "Email Address",
      "Phone (Mobile Preferred)" || '|' AS "Phone (Mobile Preferred)",
      "Date of Birth" || '|' AS "Date of Birth",
      Gender || '|' AS Gender,
      Address || '|' AS Address,
      City || '|' AS City,
      State || '|' AS State,
      Zip || '|' AS Zip,
      "Time Zone" || '|' AS "Time Zone",
      Budget || '|' AS Budget,
      "Rides Limit" || '|' AS "Rides Limit",
      "Language Preference" || '|' AS "Language Preference",
      "Communication Preference" || '|' AS "Communication Preference",
      TO_CHAR("Effective Date", 'MM/DD/YYYY') || '|' AS "Effective Date",
      COALESCE("Expiration Date", '') AS "Expiration Date",
      ROW_NUMBER() OVER (
        PARTITION BY
          "Member"
        ORDER BY
          "Expiration Date" ASC
      ) || E'\r\n' AS relevant
    FROM
      alldata
    WHERE
      DATE_PART('year', "Effective Date") = DATE_PART('year', CURRENT_DATE) + 1
  ),
  bothyears AS (
    SELECT
      *
    FROM
      currentyeardata
    WHERE
      relevant = 1 || E'\r\n'
    UNION
    SELECT
      *
    FROM
      nextyeardata
  ),
  Fileforexport AS (
    SELECT
      'Plan ID|' AS "Plan ID",
      'Plan Name|' "Plan Name",
      'Member|' "Member",
      'First Name|' "First Name",
      'Last Name|' "Last Name",
      'Email Address|' "Email Address",
      'Phone (Mobile Preferred)|' "Phone (Mobile Preferred)",
      'Date of Birth|' "Date of Birth",
      'Gender|' "gender",
      'Address|' "address",
      'City|' "city",
      'State|' "state",
      'Zip|' "zip",
      'Time Zone|' "Time Zone",
      'Budget|' "budget",
      'Rides Limit|' "Rides Limit",
      'Language Preference|' "Language Preference",
      'Communication Preference|' "Communication Preference",
      'Effective Date|' "Effective Date",
      'Expiration Date' || E'\r\n' "Expiration Date"
    UNION ALL
    SELECT
      "Plan ID",
      "Plan Name",
      "Member",
      "First Name",
      "Last Name",
      "Email Address",
      "Phone (Mobile Preferred)",
      "Date of Birth",
      "gender",
      "address",
      "city",
      "state",
      "zip",
      "Time Zone",
      "budget",
      "Rides Limit",
      "Language Preference",
      "Communication Preference",
      "Effective Date",
      "Expiration Date" || E'\r\n'
    FROM
      bothyears
  )
SELECT
  *
FROM
  alldata
  -- SELECT DISTINCT
  --   "Expiration Date"
  -- FROM
  --   fileforexport