## SonarQube

## Start SonarQube
In docker/sonarqube directory, run:
```bash
docker-compose up -d
```

## Setup SonarQube
go to https://localhost:9000 to view the sonarqube app
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
mvn clean verify && mvn -e sonar:sonar -Dsonar.projectKey=Datawave -Dsonar.host.url=http://localhost:9000 -Dsonar.login=<TOKEN>
```

Run the new sonar.sh script in your checked out datawave repository.  When it is done, return to the browser and you can see the results of your scan.

## Stop SonarQube
In docker/sonarqube directory, run:
```bash
docker-compose stop
```
You should do this before closing down for the day, or if you are done trying to do analysis.
