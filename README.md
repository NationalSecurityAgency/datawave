### Table of Contents
- [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Build/Run the Site](#build-and-run-the-site)
    - [Dev vs Prod Modes](#development-mode-vs-production-mode)
- [Site Maintenance](#site-maintenance-for-new-datawave-releases)

# Getting Started

## Prerequisites

- **Ruby** <https://rvm.io>

  ```bash
  # YMMV. See rvm.io for details 
  
  $ curl -sSL https://get.rvm.io | bash -s stable
    ...
  $ export PATH="$PATH:$HOME/.rvm/bin"
  
  $ source $HOME/.rvm/scripts/rvm
  ```
- **Jekyll** <https://jekyllrb.com/docs/quickstart/>

  ```bash
  # YMMV. See jekyllrb.com for details
  
  $ gem install jekyll bundler
    ...
  ```
- **Site Source** <https://github.com/nationalsecurityagency/datawave/tree/gh-pages>

  ```bash
  $ git clone --single-branch \
    --branch gh-pages \
    https://github.com/NationalSecurityAgency/datawave.git \
    datawave-gh-pages
    ...
  
  $ cd datawave-gh-pages
  
  $ bundle install
    ...
  ```

## Build and run the site

```bash
 $ cd datawave-gh-pages

 # This will build and run the site using the preview server with an
 # overridden baseurl value as required (see _config.yml), and will
 # auto-sync any site changes via the '--watch' option...

 $ bundle exec jekyll serve --baseurl '' --watch
   ...
```

## Development Mode vs Production Mode

When you build and run the site locally you'll be in development mode by default. Or you can
set your environment explicitly, if needed

```
 JEKYLL_ENV=development
```

Dev mode will enable display of **TODO**, **WIP**, and other dev-related tags throughout the site.
For example, new dev-mode content could be added to the site as follows

```
 ...
 {% if jekyll.environment != 'production' %}
   <h1>I'm in development!</h1> 
 {% endif %}
 ...
```

If you want to build the site as rendered on GitHub, then you must enable production mode in your environment
prior to building.

E.g.,

```bash
JEKYLL_ENV=production bundle exec jekyll serve --baseurl '' --watch
```

# Site Maintenance for New DataWave Releases

For site changes pertaining to minor and patch releases of DW, you can probably skip straight to step 9 below, i.e., for
non-structural changes and for minor updates to existing documentation within the gh-pages site.

For site updates related to a new major release, structural changes are required in order to preserve access to older
docs versions. In this case, you'll need to start with step 1 below.

### Helpful Scripts

- To automate steps 1 through 6 below for a new major release, you may run the [prep-next-major-release-docs](scripts/prep-next-major-release-docs) 
  script. (Steps 7 through 10 must be performed manually, at least for now)

- To automate publishing of *Project News* related to any new DataWave tagged release - major, minor, or other - use the
  [publish-new-releases](scripts/publish-new-releases) script.

## Steps to Update the Site for New Major Releases

1. Assuming *8.x* is the next major release, copy the existing _docs-latest (*7.x*) to a new collection directory
   ```
   cp -a _docs-latest _docs-7x
   ```
   **Note**: maintaining the underscore prefix is important to Jekyll

2. Delete the *redirect* files from the old version. We want to maintain only the ones in _docs-latest/
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
