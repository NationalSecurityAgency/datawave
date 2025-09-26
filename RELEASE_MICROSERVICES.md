# Releasing Microservices

The release process for DataWave Microservices is handled through a GitHub Actions workflow defined in `.github/workflows/microservice-release.yml`. This process currently handles one up patch releases or releases that contain a suffix (e.g. 1.0.0, 1.0.0-RC1).

## Preconditions

Before starting a release, ensure that:
- You have access to the appropriate repositories and branches.
- All code changes to be included in the release are merged and tested.

## Release Workflow Overview

1. **Access the Repository**: Navigate to the DataWave repository on GitHub.
   - Open your web browser and go to `https://github.com/NationalSecurityAgency/datawave`.
   - Ensure you are logged in with the appropriate permissions.

2. **Trigger the Workflow**:
   - Click on the "Actions" tab in the navigation bar.
   - In the list of workflows, find and click on "Release Microservices".
   - Click on "Run workflow" at the top-right of the workflow page.

3. **Input Parameters**:
   - **projectToRelease**: Choose the specific microservice or package to release. Options include:
     - `microservices/starters/audit`
     - `microservices/services/accumulo/api`
     - ... (and others as defined in the workflow).
   - **finalRelease**: Choose `true` if this is the final release, otherwise choose `false`. (A Final release will release the current patch version of the service. For example, if the poms are set to 1.0.0-SNAPSHOT, a final release will release 1.0.0, and set the poms to 1.0.1-SNAPSHOT)
   - **modifier**: Enter the version modifier, e.g., `RC1`. (This is ignored if finalRelease is true)

4. **Run the Workflow**:
   - After filling in all inputs, click "Run workflow" to start the release process.

5. **Post-Release**:
   - Check the workflow run logs for any errors.
   - Verify that the release has been tagged and pushed by viewing the tags and commits in the repository.

## Additional Notes

- The workflow handles both final and non-final releases.
- Ensure that secrets for container registry credentials (`USER_NAME`, `ACCESS_TOKEN`) are correctly set in the repository's secrets.
- Review the `.github/workflows/microservice-release.yml` file if custom configurations are needed for specific microservices.

