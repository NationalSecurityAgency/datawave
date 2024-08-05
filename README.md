# Getting Started

## Prerequisites

- Ruby: <https://rvm.io>
- Jekyll: <https://jekyllrb.com/docs/quickstart/>
- Site Source: <https://github.com/nationalsecurityagency/datawave>

## Build and run the site

```bash
 # Clone datawave and pull/checkout only the gh-pages branch...
 
 $ git clone --single-branch \
   --branch gh-pages \
   https://github.com/NationalSecurityAgency/datawave.git \
   datawave-gh-pages
   ...
 $ cd datawave-gh-pages
  
 # Build and run the site using the preview server with overridden baseurl (see baseurl configuration
 # notes in _config.yml for more info), and auto-sync site changes via '--watch' option ...
  
 $ bundle update # Optional
 $ bundle exec jekyll serve --baseurl '' --watch
 
 # By default, JEKYLL_ENV=development (see details below)
 
 # Now browse to http://localhost:4000/
 
```

## Development Mode vs Production Mode

When you build/run the site locally, you'll automatically be in development mode, i.e., **JEKYLL_ENV=development**.
So, if you want to build the production site, then you must set **JEKYLL_ENV=production**, which is the value set by
GitHub when it builds and renders the site automatically (i.e., whenever new commits are pushed) 

Currently, the only difference between dev and prod mode is that dev mode will enable display of *TODO*, *WIP*,
and other dev-related *tags* and *includes* throughout the site.

To build and run locally in production mode...

```bash
JEKYLL_ENV=production bundle exec jekyll serve --baseurl '' --watch

```

For example, you can add dev-only content to the site as follows...

```
{% if jekyll.environment != 'production' %}
   <h1>I'm in development!</h1> 
{% endif %}

```
# Site Maintenance for New DataWave Releases

For doc changes pertaining to minor or patch releases of DW, you can probably skip straight to step 9 below, i.e., for
content-only, mostly non-structural changes.

For doc updates related to a new major release, structural changes are required, so you'll need to start with step 1 below.

**Note**: You may use [scripts/prep-next-major-release-docs](scripts/prep-next-major-release-docs) to automate steps 1
through 6 below. Steps 7 through 10 must be performed manually.

**Note**: To automate publishing of 'Project News' related to new DataWave releases,
see [scripts/publish-new-releases](scripts/publish-new-releases) 

1. Assuming *8.x* is the next major release, copy the existing _docs-latest (*7.x*) to a new collection directory
   ```
   cp -a _docs-latest _docs-7x
   ```
   **Note**: maintaining the underscore prefix is important to Jekyll

2. Delete the 'redirect' files from the old version. We want to maintain only the ones in _docs-latest/
   ```
   rm -f _docs-7x/*.md
   ```
   
3. Update the *redirect* files in _docs-latest to reflect the new major version
   ```
   sed -i 's/7.x/8.x/g' _docs-latest/*.md
   ```
   
4. Copy the current [_data/sidebars/docs-latest-sidebar.yml](_data/sidebars/docs-latest-sidebar.yml) to preserve
   compatibility with the old docs version
   ```
   cd _data/sidebars
   cp docs-latest-sidebar.yml docs-7x-sidebar.yml
   ```
   
5. Update [_data/sidebars/docs-latest-sidebar.yml](_data/sidebars/docs-latest-sidebar.yml) to reflect the new major version
   ```
   sed -i 's/7.x/8.x/g' _data/sidebars/docs-latest-sidebar.yml
   ```
   And make any structural modifications as needed
   
6. Update [_data/topnav.yml](_data/topnav.yml) to reflect the new version. No need to back this one up since we're only
   maintaining a single top navigation bar for all pages in the site. Topnav.yml should always reference the latest docs version
   ```
   sed -i 's/7.x/8.x/g' _data/topnav.yml
   ```
   
7. Update [_config.yml's collections](_config.yml#L71). Following existing collection examples, copy the *docs-latest*
   stanza to a new *docs-7x* stanza, and update the *docs-latest* permalink to reflect the new 8.x version

8. Update [_config.yml's frontmatter defaults](_config.yml#L109). Following existing examples, update the frontmatter
   defaults as necessary, similar to step 7 above. Make sure to configure the *scope.type*, *values.sidebar*, and
   *values.release_tag* properties appropriately for the new and old versions

9. Make version-specific content updates to anything under [_docs-latest/](_docs-latest) as needed for the DW release

10. Locally test all site changes. See the sections above for instructions on running the site
