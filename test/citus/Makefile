DATASET = companies campaigns ads clicks impressions geo_ips
CSV     = $(addsuffix .csv,$(DATASET))
DROP = DROP TABLE IF EXISTS companies, campaigns, ads, clicks, impressions, geo_ips

all: schema data ;

schema:
	psql --single-transaction -c "$(DROP)" -d hackathon
	psql --single-transaction -f company.sql -d hackathon

data: fetch
	psql -f copy.sql -d hackathon
	../../build/bin/pgloader ./data.load

fetch: $(CSV) ;

%.csv:
	curl -O https://examples.citusdata.com/mt_ref_arch/$@

.PHONY: schema data fetch
