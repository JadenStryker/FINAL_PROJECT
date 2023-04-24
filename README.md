#Processing Big Data: Final Project

##Our Purpose

Our team attempted to identify risk factors relating to heart disease deaths in United States counties. These risk factors include Air Quality, Crime Rates, and Medicare Funding. These datasets were cleaned, normalized, and joined on the `year_state_county` column.

##Directories

Our submission contains 5 main directories: 

* __ANALYSIS:__ All the analysis data. (i.e. the result of our research and work)
    * Analysis_Corr.scala 
    * AnalysisToML.csv
    * ML.scala

* __Data_ingest:__ Publically available data sourced from government health corpus data.
    * Ingestion Process

* __ETL_FOLDER:__ Our individual dataset cleaning code as well as our cleaned data. 
    * jjs815_etl_code
        * Analysis.scala
        * Clean.scala
        * jaden_scala.csv

    * ld2494_etl_code
        * Cleaning
            * MR Files
            * clean2.sh
            * PNGs

        * leo_data.tsv

    * msa8779_etl_code
        * Clean
            * Clean.scala
            * PNGs
        
        * mateus_data.csv
    
    * rs6946_etl_code
        * hw8
        * hw9
        * rahul_final_data.csv

* __Join_Code:__ Code for joining our cleaned datasets.
    * full_merger.csv
    * merger_code.scala
    * states.csv

* __Profiling_Code:__ Our individual code for profiling our datasets.
    * jjs815_profiling
    * ld2494_profiling
        * MR Files
        * analysis_1.sh
        * PNGs

    * msa8779_profiling_code
    * rs6946_profiling
      
## Building & Running Code

The code must be run in the following order: Profiling_Code -> ETL_FOLDER -> Join_Code -> ANALYSIS.

* __Scala:__ All of the scala files are "plug and play" given locally saved data files are present and access to all team-members' DataProcs is satisfied. All scala files are run on the Spark runtime in client mode.

* __Map Reduce:__ In the ld2494_* folders, there are MapReduce files that can be run using the bash scripts identifiable by their \*.sh extensions. These files will automatically run the respective jobs with the command: `bash <SCRIPT NAME>.sh`. 

In the rs6946_* folders, the MapReduce jobs can be run using the typical commands detailed on Brightspace.

All team members are ready and available to answer any questions if there are any implementation issues on the graders' ends.

## Results

The results of our analyses can be found in the ANALYSIS folder. On DataProc, the scala makes a call to the HDFS directory, where the resulting analysis files are permanently located.

## Where To Find Data

The data can be found through the original sources specified in the data ingest folder.