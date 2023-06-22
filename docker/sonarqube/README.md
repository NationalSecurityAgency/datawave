## SonarQube

## Preliminary Steps
(you *will* need to run this daily before attempting to start sonarqube - use precompose.sh script in case other things are needed)
sudo sysctl -w vm.max_map_count=262144
sudo sysctl -w fs.file-max=131072

sudo vi /etc/sysctl.conf
vm.max_map_count=262144
fs.file-max=131072

You need to run in older versions of maven.  3.8.6 works, 3.9.6 does NOT. 

## Start SonarQube
In docker/sonarqube directory, run:
```bash
./precompose.sh
docker-compose up -d
```

## Setup SonarQube
go to https://localhost:7777 to view the sonarqube app
admin/admin is the default password (change it when you first access it)

How do you want to create your project? Manually

Set project name to DataWave
(leave project key as DataWave)
Set main  branch to integration
Click Set Up

How do you want to analyze your repository? Locally

Leave the token name as "Analyze DataWave"
Set the expiration to Never Expires
Click Generate

Copy the token (spq_...), save it locally in a file called sonarqubeToken in your home directory.

Click Continue

Click Maven

Create a sonar.sh script, add it to your ~/bin dir (or anywhere that is on your configured path)
```bash
mvn clean verify && mvn -e sonar:sonar -Dsonar.projectKey=Datawave -Dsonar.host.url=http://localhost:7777 -Dsonar.login=<TOKEN>
```

Run the new sonar.sh script in your checked out datawave repository.  When it is done, return to the browser and you can see the results of your scan.

## Stop SonarQube
In docker/sonarqube directory, run:
```bash
docker-compose stop
```
You should do this before closing down for the day, or if you are done trying to do analysis.
