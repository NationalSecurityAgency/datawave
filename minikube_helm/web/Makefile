#########################
# Environment Variables #
#########################

HELM_CHART_NAME ?= datawave
HELM_CHART_VERSION ?= 1.1.2
NEXUS_ADMIN_PASSWORD ?= $(shell bash -c 'read -p "Nexus Password: " password; echo $$password')
NEXUS_ADMIN_USER ?= $(shell bash -c 'read -p "Nexus User: " user; echo $$user')
NEXUS_HELM_REPOSITORY ?= http://10.117.153.38:9091/nexus/repository/helm-datawave/

###########################
# Main Lifecycle Commands #
###########################

test:
	@echo "Testing Helm Chart via Lint..."
	@helm lint .

	@echo "Debugging Helm Chart Template(s)..."
	@helm template --debug .

package: test
	@echo "Packaging Helm Chart Archive..."
	@helm package .

release: package

	@echo "Uploading Helm Chart to Nexus Repository..."
	@curl -u ${NEXUS_ADMIN_USER}:${NEXUS_ADMIN_PASSWORD} -v -k \
		${NEXUS_HELM_REPOSITORY}/ --upload-file ${HELM_CHART_NAME}-${HELM_CHART_VERSION}.tgz

##################################
# Development Lifecycle Commands #
##################################

setup:
	@echo "Verifying that Development Environment is Ready..."
	@./setup.sh

dashboard: setup
	@echo "Launching the Kubernetes Dashboard..."
	@minikube dashboard

deploy: setup
	@echo "Deploy Test Helm Release..."
	@helm install ${DEBUG_RELEASE} .

destroy:
	@echo "Destroying Test Helm Release..."
	@helm uninstall ${DEBUG_RELEASE}

update:
	@echo "Upgrading Test Helm Release..."
	@helm upgrade ${DEBUG_RELEASE} .
