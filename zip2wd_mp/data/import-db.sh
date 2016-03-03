#!/usr/bin/env bash

if [[ $# -eq 0 ]] ; then
    printf 'usage: %s <year>\n' $_ 
    exit 1
fi

FILE=$1.csv

if [ -f $FILE ];
then
	echo "GNCH Daily data for year ${1} downloaded"
else
	echo "Download GHCN daily data for year ${1} from FTP server..."
	wget -c ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/$1.csv.gz
	echo "Extract data $1.csv.gz..."
	gunzip $1.csv.gz
fi

SQL=$(cat <<EOF
CREATE TABLE IF NOT EXISTS \`ghcn_${1}\` (
\`id\` VARCHAR(12) NOT NULL,
\`date\` VARCHAR(8) NOT NULL,
\`element\` VARCHAR(4) NULL,
\`value\` VARCHAR(6) NULL,
\`m_flag\` VARCHAR(1) NULL,
\`q_flag\` VARCHAR(1) NULL,
\`s_flag\` VARCHAR(1) NULL,
\`obs_time\` VARCHAR(4) NULL);
create index if not exists idx_id_time on ghcn_${1} (id, date);
PRAGMA journal_mode = MEMORY;
PRAGMA synchronous = OFF;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 500000;
.mode csv
.headers off
.separator ","
.import ${1}.csv ghcn_${1}
EOF
)

echo "Import ${1}.csv to database ghcn_${1}.sqlite3...(take a few minutes)"
echo "${SQL}" | sqlite3 ghcn_${1}.sqlite3
echo "Done"
