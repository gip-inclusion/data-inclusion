# Deployment

* The project is deployable on the Scalingo platform.
* Each service (pipeline, api, etc.) is deployed in its own application.
* It is made possible using the [`PROJECT_DIR`](https://doc.scalingo.com/platform/getting-started/common-deployment-errors#project-in-a-subdirectory) env variable defined in each app.
* Services are configured through the environment.