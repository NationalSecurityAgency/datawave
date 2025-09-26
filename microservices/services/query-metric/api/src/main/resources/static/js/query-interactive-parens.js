function highlight(line) {
    line.style.backgroundColor = "#1BFC06";  // highlighter green
    line.style.textDecoration = "none";
    line.style.color = "#000000";
}

function unhighlight(line) {
    line.style.backgroundColor = "transparent";
}

// Function to make the provided query or query plan's parenthesis interactive
function interactiveParens(query = '', isQueryPlan = false, metricNum) {
    const lines = query.split(/\r?\n/);
    for (let i = 0; i < lines.length; i++) {
        // If the line is 0 or more spaces followed by and ending with an open paren, find its matching closing paren (on a different line)
        if (/^\s*\(+$/.test(lines[i])) {
            for (let j = i + 1; j < lines.length; j++) {
                var id_line_i, id_line_j;
                if (isQueryPlan) {
                    id_line_i = `query-plan${metricNum}-line${i + 1}`;
                    id_line_j = `query-plan${metricNum}-line${j + 1}`;
                } else {
                    id_line_i = `query${metricNum}-line${i + 1}`;
                    id_line_j = `query${metricNum}-line${j + 1}`;
                }
                // if this is true, we have found the matching paren
                if (lines[j].replaceAll(')', '(').substring(0, lines[i].length) === lines[i]) {
                    lines[j] = `<a class="a-no-style" href=#${id_line_i} id=${id_line_j} onmouseover="highlight(this); highlight(document.getElementById('${id_line_i}'));" onmouseout="unhighlight(this); unhighlight(document.getElementById('${id_line_i}'));">${lines[j]}</a>`;
                    break;
                }
            }
            lines[i] = `<a class="a-no-style" href=#${id_line_j} id=${id_line_i} onmouseover="highlight(this); highlight(document.getElementById('${id_line_j}'));" onmouseout="unhighlight(this); unhighlight(document.getElementById('${id_line_j}'));">${lines[i]}</a>`;
        }
        // Otherwise, if the line just includes an open paren, match all parens on this line
        else if (lines[i].includes('(') && /^<a/.test(lines[i]) === false) {
            let lineAsArr = lines[i].split('');
            let numParens = 0;
            for (let k = 0; k < lineAsArr.length; k++) {
                if (lineAsArr[k] === '(') {
                    numParens++;
                    let count = 1;
                    for (let m = k + 1; m < lineAsArr.length; m++) {
                        var id_line_i_open_paren, id_line_i_close_paren;
                        if (isQueryPlan) {
                            id_line_i_open_paren = `query-plan${metricNum}-line${i + 1}-open-paren${numParens}`;
                            id_line_i_close_paren = `query-plan${metricNum}-line${i + 1}-close-paren${numParens}`;
                        } else {
                            id_line_i_open_paren = `query${metricNum}-line${i + 1}-open-paren${numParens}`;
                            id_line_i_close_paren = `query${metricNum}-line${i + 1}-close-paren${numParens}`;
                        }
                        if (lineAsArr[m] === ')') count--;
                        else if (lineAsArr[m] === '(') count++;
                        // If count is 0, we have found the matching closing paren
                        if (count === 0) {
                            lineAsArr[m] = `<a class="a-no-style" href=#${id_line_i_open_paren} id=${id_line_i_close_paren} onmouseover="highlight(this); highlight(document.getElementById('${id_line_i_open_paren}'));" onmouseout="unhighlight(this); unhighlight(document.getElementById('${id_line_i_open_paren}'))">)</a>`;
                            break;
                        }
                    }
                    // highlight paren and matching closing paren
                    lineAsArr[k] = `<a class="a-no-style" href=#${id_line_i_close_paren} id=${id_line_i_open_paren} onmouseover="highlight(this); highlight(document.getElementById('${id_line_i_close_paren}'));" onmouseout="unhighlight(this); unhighlight(document.getElementById('${id_line_i_close_paren}'))">(</a>`;
                }
            }
            lines[i] = lineAsArr.join('');
        }
    }
    // Update the tables query-plan or query for metric number metricNum
    if (isQueryPlan)
        document.getElementById(`query-plan${metricNum}`).innerHTML = lines.join('\n');
    else
        document.getElementById(`query${metricNum}`).innerHTML = lines.join('\n');
}
