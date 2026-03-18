#!/usr/bin/env bash

# Set default values
range="0 1 2"
duration=3
concurrency=100

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -r)
        range="$2"
        shift 2
        ;;
        -d)
        duration="$2"
        shift 2
        ;;
        -c)
        concurrency="$2"
        shift 2
        ;;
        --help)
        echo "Usage: $0 [-r range] [-d duration] [-c concurrency]"
        echo "  range: A space-separated list of indices corresponding to the AZs to test (default: '0 1 2')"
        echo "  duration: Duration of each test run in seconds (default: 3)"
        echo "  concurrency: Number of concurrent requests to simulate (default: 100)"
        exit 0
        ;;
        *) # unknown option
        shift
        ;;
    esac
done
#AZS=(usw2-az1 usw2-az2 usw2-az3)
#AZS=(usw2-az1 usw2-az2 usw2-az3)
AZS=(us-west1-a us-west1-b us-west1-c)

# application.conf is located in targe/classes after compilation, so we can edit it in place before each run
if [ -d "target/classes" ]; then
    dir=$(pwd)
    cd target/classes && ln -sf ../../application.conf
    cd $dir 
fi

# Run the example 3 times, each time with a different AZ in the local-rack setting
for n in ${range}; do
printf "Setting local-rack to ${AZS[n]} in application.conf\n"
sed -i "s/local-rack = .*/local-rack = \"${AZS[n]}\"/" application.conf
printf "\nRunning the AWS Zone Aware example with specific AZ: ${AZS[n]}\n"
java -cp "target/classes:target/zone-aware-1.0-SNAPSHOT.jar" zoneAware -c ${concurrency} -d ${duration} 
done
