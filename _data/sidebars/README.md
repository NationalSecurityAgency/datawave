# Sidebar Notes

* The goal here is to have sidebar setup for our various pages and collection types be dynamic and data-driven

* Typically, we'll have a distinct sidebar for each released major version of DataWave, as each major version will
  generally have a distinct docs "collection" type defined

* The format of the DataWave docs sidebar file names should be as follows: *docs-<majorVersion>x-sidebar.yml* for a
  prior major version, or *docs-latest-sidebar.yml* for the sidebar of latest version 
  * No periods are allowed in the file name (excluding the .yml extension), because the name itself must be a valid YAML identifier, or else
    Jekyll will fail to build the site correctly  

* The selection of a particular sidebar for each web page is typically defaulted in *_config.yml* based on the page's
  collection type. However, the sidebar may be overridden, if needed, in the frontmatter section of the given
  page

* The code in *_includes/sidebar.html* will build an accordion-style menu based on whatever data is provided in the sidebar's YAML file
