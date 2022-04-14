#!/bin/bash
if [ $# -eq 0 ]; then
    echo "Usage ./generate_all_workloads.sh \$workload_directory"
    exit 0
fi

# workload directory
output_dir=$1
mkdir $output_dir
CUR_DIR=$(pwd)
## integer keys
KEY_TYPE=randint
old_skew=0.99
#for new_skew in 0.5 0.7 0.9 1.1 1.2 1.3; do
for new_skew in 0.99; do
    sed -i 's/public static final double ZIPFIAN_CONSTANT = '$old_skew'/public static final double ZIPFIAN_CONSTANT = '$new_skew'/g' ~/YCSB/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java;
    sed -i 's/public static final double USED_ZIPFIAN_CONSTANT = '$old_skew'/public static final double USED_ZIPFIAN_CONSTANT = '$new_skew'/g' ~/YCSB/core/src/main/java/site/ycsb/generator/ScrambledZipfianGenerator.java
    old_skew=$new_skew
    for WORKLOAD_TYPE in a b c e _mixed; do
        echo workload${WORKLOAD_TYPE} > workloads/workload_config.inp
        echo ${KEY_TYPE} >> workloads/workload_config.inp
        echo ${new_skew} >> workloads/workload_config.inp
        python workloads/gen_workload.py workloads/workload_config.inp $output_dir
        mv $output_dir/load_${KEY_TYPE}_workload${WORKLOAD_TYPE}_${new_skew} $output_dir/load${WORKLOAD_TYPE}_int_100M_${new_skew}.dat
        mv $output_dir/txn_${KEY_TYPE}_workload${WORKLOAD_TYPE}_${new_skew} $output_dir/txns${WORKLOAD_TYPE}_int_100M_${new_skew}.dat
    done
    old_skew=$new_skew
done


"
# email keys
cd $output_dir
wget https://archive.org/download/300MillionEmailDatabase/300%20million%20email%20database.rar
unrar x '300 million email database.rar'
cd 300\ MILLION\ EMAIL\ DATABASE/worldwide
unrar x Country.rar
unrar x 'Mail Servers.rar'
unrar x Unclassified.rar
unrar x China.rar
unrar x Commercial.rar
EMAIL_DIR=$(pwd)
rm *.rar
mv $output_dir/300\ MILLION\ EMAIL\ DATABASE $output_dir/email_database
EMAIL_DIR=$output_dir/email_database/worldwide
cd $EMAIL_DIR
echo $(pwd)
cd $CUR_DIR
mkdir workloads/bin
g++ -O3 -std=c++17 -march=native -o workloads/bin/extract_email workloads/extract_email.cpp -lstdc++fs
g++ -O3 -std=c++17 -march=native -o workloads/bin/parse_email workloads/parse_email.cpp
./workloads/bin/extract_email ${EMAIL_DIR}
./workloads/bin/parse_email ${EMAIL_DIR}/raw_emails.dat $output_dir
#rm ${EMAIL_DIR}/raw_emails.dat

## url keys
cd $output_dir
wget http://data.law.di.unimi.it/webdata/uk-2007-05/uk-2007-05.urls.gz
gzip -d uk-2007-05.urls.gz
cd $CUR_DIR
g++ -O3 -std=c++17 -march=native -o workloads/bin/parse_url workloads/parse_url.cpp
./workloads/bin/parse_url $output_dir/uk-2007-05.urls $output_dir
rm $output_dir/uk-2007-05.urls
"
