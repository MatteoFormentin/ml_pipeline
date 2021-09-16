# Design and implementation of a software pipeline for machine learning on streaming data

## *Msc Thesis in Computer Science and Enginnering of Matteo Formentin, Politecnico di Milano, 2021*

## Getting started

### Folder structure of the source repo

The repository folders are organised as it follows:

* demo_links: Contains the CSV logs produced by ten different links to perform testing.

* docker: Contains the docker-compose file that defines the pipeline, the configurations file of all the pipeline software and the dockerfile to build spark images.

* ml-models: Contains the dataset and script to train the machine learning model that will be deployed by the pipeline.

* netsim: Contains the sources of the network simulator used to emulate the log sources.

* spark: Contains the source of the Spark application developed to handle the classification in the pipeline with the corresponding dockerfile.

### Docker compose file
The docker-compose file contains the specification of all the containers that the engine should create to run the pipeline. Each container is an entry on the file along with its configuration. The most important are:

* image or build: Specify the image to run the container. If image is used the corresponding image is downloaded from the docker registry, if build is used the provided dockerfile is built.

* environment: ENV variables provided to the container to configure the running application.

* volumes: Definition of the modules that are mounted inside the container to provide configuration file or persistence.

* ports: Defines the port forwarding in the format host_port:container_port for external access. 

* scale: number of containers to run from that image, default is one. Supported only by spark-worker. When other instances of the same application requires a different configuration a new entry in the compose file with the proper configuration should be provided. Also, when a pipeline application is scaled remember to add to the Metricbeat configuration the new containers to monitor.

We provide two docker-compose files:

* docker-compose-dev.yml: Run the development version of the pipeline, a minimal one with only one instance per application and no spark cluster. PySpark should be installed on the host machine. Can be run on a machine with at least 8 GB of docker dedicated RAM.

* docker-compose-production.yml: Run the complete version of the pipeline, with all major applications replicated. Require at least 32 GB of dedicated RAM to run, the more the better.

The following containers are defined inside the production file:

* es01, es02, es03: Elasticsearch cluster, expose port 9200 for testing purposes. Each node requires a proper configuration, so if the scale is needed another entry (e.g es04,..) should be added.

* kib01, kib02, kib03: Kibana. Expose the web interface on port 5601, and it is the main access for data visualisation and pipeline management.

* ls01, ls02, ls03: Logstash.

* metricbeat: Metricbeat.

* zookeeper: Zookeeper is required by Kafka to manage the cluster.

* kafka01, kafka02, kafka03: Kafka brokers. Expose port 9093 to 9095 (one per broker) for injection of log by netsim.

* spark-master: Spark master. Expose port 8080 for access to the web UI.

* spark-worker: Spark workers. Can be scaled by changing the scale field.

* spark-driver: Spark Driver. Runs the application that executes the machine learning models.

Spark images are build since they are not available from the registry, while the others are downloaded on the first run. In the development version, the same containers are defined without the final progressive number and Spark cluster is not provided for performance reasons.

### Run in production

Run the pipeline in production mode. This runs all the application in cluster mode, enabling replication. 
Note that this requires a powerful machine to run.

1. Clone this repo on the host machine and change the working directory inside:

    ``` bash
    git pull https://github.com/MatteoFormentin/ml_pipeline
    cd ml_pipeline
    ```

2. Provision the host machine and start the pipeline: 

    * If using AWS Ubuntu Image or a Linux machine, run:

        ``` bash
        sudo bash script/aws_provision.sh
        ```

        This will automatically provision a clean machine by installing Docker and performing some configuration, downloading images and finally starts the pipeline. From now, to run again the pipeline:

        ``` bash
        sudo bash script/run_production.sh
        ```

        To stop the pipeline:

        ``` bash
        sudo docker-compose -f docker/docker-compose-production.yml down
        ```

    * On other operating systems, manual setup is required:

        * Install docker and docker-compose. See docker documentation for instructions.

        * Modify permissions of Metricbeat configuration file:

        ``` bash
        chmod 644 docker/config/metricbeat/production/metricbeat.yml
        ```

        * Create a .env file with the followng content:

        ``` bash
        PUBLIC_IP=pipeline_host_public_ip
        ```

        Where pipeline_host_public_ip must be the public reachable IP address of the pipeline host if netsim and the pipeline run on different networks, a private IP if on the same subnet, or localhost if both on the same machine.

        * Run the pipeline:

        ``` bash
        docker-compose -f docker/docker-compose-production.yml --env-file .env up -d --build
        ```

3. Install the requirement for netsim:

    * Install Python 3 and pip. See Python documentation for instructions.

    * Install required packages:

        ``` bash
        pip install -r netsim/requirements.txt
        ```

4. Import the dashboards inside Kibana:

    * Open *http://pipeline_host_public_ip:5601*

    * Click on the top left to open the menu

    * Go to *Stack Management* -> *Saved Object* 

    * Click import on the right side, then choose the file dashboards.ndjson that can be found inside the folder docker/config/kibana

    * Click on "Done"

5. Access the Basic Visualisation Dashboard:

    * Click on the top left to open the menu

    * Go to *Dashboard* -> *Basic Visualisation*

6. Start a demo simulation:

    ``` bash
    python3 netsim simulate "demo_links" pipeline_host_public_ip:9093 siae-pm --interval=10 --limit=10
    ```

### Run in development

1. Clone this repo on the host machine and change the working directory inside:

    ``` bash
    git pull https://github.com/MatteoFormentin/ml_pipeline
    cd ml_pipeline
    ```

2. Install requiremets and start the pipeline and the local Spark install: 

    ``` bash
    pip install pyspark
    docker compose -f docker/docker-compose-dev.yml up -d --build
    python3 spark/ann_model/src/ann_model_batch.py 
    ```

3. Install the requirement for netsim:

    * Install Python 3 and pip. See Python documentation for instructions.

    * Install required packages:

        ``` bash
        pip install -r netsim/requirements.txt
        ```

4. Import the dashboards inside Kibana:

    * Open *http://pipeline_host_public_ip:5601*

    * Click on the top left to open the menu

    * Go to *Stack Management* -> *Saved Object* 

    * Click import on the right side, then choose the file dashboards.ndjson that can be found inside the folder docker/config/kibana

    * Click on "Done"

5. Access the Basic Visualisation Dashboard:

    * Click on the top left to open the menu

    * Go to *Dashboard* -> *Basic Visualisation*

6. Start a demo simulation:

    ``` bash
    python3 netsim simulate "demo_links" pipeline_host_public_ip:9093 siae-pm --interval=10 --limit=10
    ```

## Run custom simulation with Netsim

### Netsim command line reference

The command exec netsim with the ten logs inside demo_link folder. It has the following usage:

python3 netsim mode log_folder kafka_url topic

* mode: Run netsim in split or simulation mode.
* folder: Source folder of the CSV logs.
* kafka_url: URL of the Kafka broker.
* topic: Kafka topic destination of the log, as default the pipeline is subscribed to siae-pm} topic.
* ---interval: Optional flag that set the delay in seconds between the ingestion of log for each producer. Default 900 (15 minutes)
* ---limit: Optional flag that limits the number of producers to run.

### Prepare dataset for Netsim

Historical logs from the radio network are exported as a unique file with log ordered by link identifier and date and time. To be able to use them in netsim, that file needs to be split into one CSV per link. To do this task netsim in split mode can be used:

python3 netsim split input_csv out_folder column_name

* split: Run netsim in split mode.
* input_csv: Path to input file to split.
* out_folder: Path to folder where split files are saved
* column_name: Name of the column that identifies the link. For SIAE-PM dataset use idlink.

### Run simulation

After splitting, the simulation can be started using simulate mode providing the out_folder as source.
