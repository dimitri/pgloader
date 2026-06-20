SELECT count(*) FROM places;
SELECT usps, fips, fips_code, "LocationName"
  FROM places
 ORDER BY usps, fips_code
 LIMIT 5;
