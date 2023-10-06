---
title: Project News
permalink: /news/index.html
---
<div class="home">
    <div class="post-list">
        {% assign published = site.posts | where:"draft",false %}
        {% for post in published limit:15 %}
            <h2>
              <a class="post-link" href="{{site.baseurl}}{{ post.url }}">
                {{ post.title }}
                {% if post.category == "release" %}
                  &nbsp;Released
                {% endif %}
              </a>
            </h2>
            <span class="post-meta">{{ post.date | date: "%b %-d, %Y" }} /
              {% for tag in post.tags %}
                <a href="{{site.baseurl}}{{ "/pages/tags/" | append: tag }}">{{tag}}</a>{% unless forloop.last %}, {% endunless%}
              {% endfor %}
            </span>
            <p>
              {% if page.summary %}
                {{ page.summary | strip_html | strip_newlines | truncate: 160 }}
              {% else %}
                {{ post.content | truncatewords: 35 | strip_html }}
              {% endif %}
            </p>
            <hr />
        {% endfor %}
        <p>See more posts from the <a href="{{site.baseurl}}/news/archive">News Archive</a></p>
        <p><a href="{{site.baseurl}}/feed.xml" class="btn btn-primary navbar-btn cursorNorm" role="button">RSS Subscribe{{tag}}</a></p>
    </div>
</div>

