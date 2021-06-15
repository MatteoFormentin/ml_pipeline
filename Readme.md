# Data ingestion pipeline for real-time ML

### *Msc Thesis in Computer Science and Enginnering of Matteo Formentin, Politecnico di Milano, 2021*
  
<br />

   
## Getting started

### Run in production
Run the pipeline in production mode. This run all the application in cluter mode, enabling replication. 
Note that this require a powerful machine to run.

1. Clone this repo on the host machine and change working direectory inside: 

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

    * On other operatting systems, manual setup is required:
        * Install docker and docker-compose. See docker documentation for instructions.

        * Modify permissions of Metricbeat configuration file:

            ``` bash
            chmod 644 docker/config/metricbeat/production/metricbeat.yml
            ```

        * Create a .env file with the followng content:

            ``` bash
            PUBLIC_IP=pipeline_host_public_ip
            ```

            Where pipeline_host_public_ip must be the public reachble IP address of the pipeline host if netsim and the pipeline runs on different networks, a private ip if on the same subnet, or localhost if both on the same machine.

        * Run the pipeline:

            ``` bash
            docker-compose -f  docker/docker-compose-production.yml  --env-file .env up -d --build
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
    * Click import on the to right side, then choose the file dashboards.ndjson that can be found inside the folder docker/config/kibana
    * Click on "Done"

5. Access the Basic Visualisation Dashboard:
    * Click on the top left to open the menu
    * Go to *Dashboard* -> *Basic Visualisation*

6. Start a demo simulation:

    ``` bash
    python3 netsim simulate "demo_links" 15.160.22.36:9093 siae-pm --interval=10 --limit=10
    ```

