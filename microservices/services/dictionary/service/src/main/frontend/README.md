# <p style="margin-bottom: 0px">Datawave Data Dictionary Version 2 <img src="./public/favicon.ico" style="width: 2em; height: 2em; vertical-align: middle;"></p>

### Project Overview
Data Dictionary Version 2 is a new microservice designed to enhance the original Data Dictionary. It leverages an updated tech stack to incorporate features that facilitate data analysis and provide quick query functions. Below are detailed instructions on how to install, run, and configure this project. This README will guide you through the process of starting and running the project.

### Tech Stack Overview
The tech stack for this project includes **Vue.js (v3.0)** with **Vite** for building the frontend and enabling hot module replacement. **Quasar (v2.8)** is used for constructing the main table and handling data insertion. The backend is powered by **Java 11** with **Spring Boot** for serving data through RESTful methods, while **Node.js (v16+)** and **NPM** manage packages and dependencies. Lastly, **Apache Accumulo** provides data storage for visualization.

## Running the Application
This section provides the installation details and outlines the steps to run Data Dictionary V2 in production-ready mode. Additionally, it includes instructions for making rapid frontend changes using Quasar's Dev Mode with Vue. While both modes can be operated independently, it is strongly recommended to conduct thorough testing in production mode, as it closely simulates the deployment environment.

### Instructions for Production-Ready Container Mode
1. To run the application in production mode, first ensure Java 11 and Maven is installed. Next, clone the [Datawave Repository](https://github.com/NationalSecurityAgency/datawave) into your preferred directory, then navigate into it by running the following command(s):

    ```
    git clone git@github.com:NationalSecurityAgency/datawave.git
    cd datawave
    ```

2. Make sure you are on the integration branch for Datawave, as the Dictionary service depends on the Configuration and Authorization microservices. Then run the following to build the project. Ensure that the cache is disabled in the maven command:

    ```
    # Build the datawave project and produce images for each microservice:

    mvn -Pcompose -Dmicroservice-docker -Dquickstart-docker -Ddeploy -Dtar -Ddist clean install -DskipTests -Dmaven.build.cache.enabled=false

    # Spin up all the containers for the microservice after full datawave build:

    cd /datawave/docker
    docker compose --profile dictionary --profile quickstart up -d
    ```

3. Now if you go to https://localhost:8643/dictionary/data/v2/#/ you should be able to see the Data Dictionary Version 2 and if you go to https://localhost:8643/dictionary/data/v1/ you will be able to see the predecessor. If you can't reach it due to client certificate errors please download the [p12 certificate](https://github.com/NationalSecurityAgency/datawave-spring-boot-starter/raw/refs/heads/main/src/main/resources/testUser.p12). Once downloaded, on your browser (this is assuming you are using Chrome but should be similar with other browsers): go to _Privacy and Secrurity_, then select _Security_, then scroll down to _Manage Certificates_, under the _Your Certificates_ tab, click import and select the p12 file you downloaded. The password should be "ChangeIt", and now you are paired with the authorization service. It is highly recommended you change this certificate for yourself, more information to do this can be found at this [README](https://github.com/NationalSecurityAgency/datawave-microservices-root/blob/main/README.md#getting-started) for the Datawave External Services.

##### To Make a Quick Change
1. If you would like to make a quick change either to the backend or frontend of the service, run the following:

    ```
    # First go to the docker directory and take down the containers:

    cd /datawave/docker
    docker compose --profile dictionary --profile quickstart down dictionary

    # Enter only the Dictionary Service Directory and rebuild the jar and image:

    cd /datawave/microservices/services/dictionary/

    mvn -Pcompose -Dmicroservice-docker -Dquickstart-docker -Ddeploy -Dtar -Ddist clean install -DskipTests -Dmaven.build.cache.enabled=false

    # Go back to the docker directory and spin up the containers again.
    cd /datawave/docker
    docker compose --profile dictionary --profile quickstart up -d dictionary
    ```
2. Then you should be able to hit this endpoint again: https://localhost:8643/dictionary/data/v2/#/

### Instructions for Quasar Dev Mode (Uses Vite.js)
1. To run this application in DEV mode, first follow the steps above to build and run the containers. This process only needs to be completed once to retrieve the necessary endpoints.
2. Next, in order to run in DEV mode, you will first need to install [Node.js and NPM](https://nodejs.org/en/download/package-manager/). Ensure you installed them by running `npm -v` and `node -v` to check their versions.
3. Then you will need to globally install the the Quasar/CLI package. This can be done using npm by running:

    ```
    npm i -g @quasar/cli
    ```
4. Once installed, you can make quick changes in Quasar's Dev Mode. Open your code editor (such as VSCode) and ensure you have the official Vue and Vite extensions installed. Then, open the Datawave project directory in your code editor. Navigate to the main repository of the Data Dictionary service in your code editor and in your terminal which is:

    ```
    cd /datawave/microservices/services/dictionary/service/src/main/frontend
    ```
5. Now, make a change to the `DataDictionary.vue` file (where most of the frontend components are housed) or any TypeScript file, such as logging a message to the terminal or modifying the color of a button. While still in this directory, execute the following command:

    ```
    quasar dev
    ```
*For more information, please refer to [Quasar's documentation](https://quasar.dev/start/quasar-cli#running-without-the-global-quasar-cli) on running in development mode.*

## Configuration Information
- `package.json`: This file defines the project's build process and manage its dependencies through NPM. They include crucial information such as project metadata, versioning, icons, and scripts for running the project in Quasar DEV mode. For instance, you will be able to see that this project uses Bootstrap Icons in order to populate some of buttons.

- `quasar.config.js`: This configuration file governs the behavior of Quasar, managing essential plugins and modules for its operation. It also integrates settings for Vite, to allow for DEV mode. Additionally, this file defines routes for specific API endpoints, such as the *banner/* endpoint, along with [other data endpoints](https://github.com/NationalSecurityAgency/datawave-dictionary-service/blob/9b0568347e360f32392d4feee662d2ddf9eacd17/README.md) used to populate rows and columns. Furthermore, it specifies routing configurations to streamline navigation within the application.

  *Please note that `package-lock.json` is not required for this project and may be removed.*

***
