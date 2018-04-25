# To work with this site locally...

## Prerequisites

- Ruby: <https://rvm.io>
- Jekyll: <https://jekyllrb.com/docs/quickstart/>
- Site Source: <https://github.com/nationalsecurityagency/datawave>

## Build and run the site

```bash
 # Checkout the gh-pages branch
 
 $ cd <DW SOURCE DIR>
 $ git checkout gh-pages (or gh-pages-dev)
 
 # Build and run site using the preview server, and auto-sync changes via --watch
  
 $ bundle update
 $ bundle exec jekyll serve --watch
 
 # Now browse to http://localhost:4000
 
```

## Development Mode vs Production Mode

When you build/run the site locally, you'll automatically be in development mode, i.e., **JEKYLL_ENV=development**.
So, if you want to build the production site, then you must set **JEKYLL_ENV=production**, as is the case on GitHub.

Currently, the only difference between dev and prod mode is that dev mode will enable display of *TODO*, *WIP*,
and other dev-related *tags* and *includes* throughout the site.

To build and run locally in production mode...

```bash
JEKYLL_ENV=production bundle exec jekyll serve --watch

```

Thus, internally, you can add dev-only elements and behaviors to the site as follows...

```
{% if jekyll.environment != 'production' %}
   I'm in development! 
{% endif %}

```