---
title: News Archive
permalink: /news/archive.html
---
<div>
<hr>

{% assign published = site.posts | where:"draft",false %}
{% assign YYYY = published[0].date | date: "%Y" %}

<h3>{{YYYY}}</h3>

{% for post in published %}

  {% assign post_year = post.date | date: "%Y" %}
  {% if post_year != YYYY %}
    {% assign YYYY = post_year %}
    <hr>
    <h3>{{ YYYY }}</h3>
  {% endif %}

  <div class="row" style="margin-top: 15px">
    <div class="col-md-1">{{ post.date | date: "%b %d" }}</div>

    <div class="col-md-10">
      <a href="{{ site.baseurl }}{{ post.url }}">
      {{ post.title }}
      {% if post.category == "release" %}
        &nbsp;Released
      {% endif %}
      </a>
    </div>

  </div>
{% endfor %}
</div>
