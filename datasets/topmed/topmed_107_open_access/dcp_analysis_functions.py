import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from lifelines import KaplanMeierFitter
from lifelines import NelsonAalenFitter
from scipy.stats import ranksums
import numpy as np
import datetime
import requests
import glob
import json
import os
from operator import add
from scipy.stats import expon

summary_order = [
    "_case_count",
    "_study_count",
    "_demographic_count",
    "_diagnosis_count",
    "_exposure_count",
    "_aliquot_count",
    "_submitted_snp_array_count",
    "_copy_number_variation_count",
    "_submitted_aligned_reads_count"
]

summary_count_headers = {
    "_case_count": "Cases",
    "_study_count": "Studies",
    "_demographic_count": "Demographic records",
    "_diagnosis_count": "Diagnosis records",
    "_exposure_count": "Exposure records",
    "_aliquot_count": "# Aliquots",
    "_submitted_aligned_reads_count": "# BAM/CRAM Files",
    "_submitted_snp_array_count": "# SNP Array Files",
    "_copy_number_variation_count": "# Copy Number Files"
}

class DiseasesTable(dict):
    ''' Represent disease table in HTML format for visualization '''

    def _repr_html_(self):
        html = []
        html.append("<table style>")
        html.append("<thead>")
        html.append("<th>Disease</th>")
        html.append("<th># Subjects</th>")
        html.append("</thead>")
        total = 0
        for key in self:
            html.append("<tr>")
            html.append("<td>%s</td>" % key)
            html.append("<td>%s</td>" % self[key])
            html.append("<tr>")
            total += self[key]
        html.append("<td>TOTAL</td>")
        html.append("<td>%s</td>" % total)
        html.append("</table>")

        return ''.join(html)

class MetricsTable(dict):
    ''' Represent metrics tables in HTML format for visualization '''

    def _repr_html_(self):
        html = []
        html.append("<table style>")
        html.append("<thead>")
        html.append("<th>Metric</th>")
        html.append("<th>Value</th>")
        html.append("</thead>")
        for key in self:
            html.append("<tr>")
            html.append("<td>%s</td>" % key)
            html.append("<td>%s</td>" % self[key])
            html.append("<tr>")
        html.append("</table>")

        return ''.join(html)

class SummaryTable(dict):
    ''' Represent result tables in HTML format for visualization '''

    def _repr_html_(self):
        html = []
        html.append("<table style>")
        html.append("<thead>")
        html.append("<th>Category</th>")
        html.append("<th>Counts</th>")
        html.append("</thead>")
        for key in summary_order:
            html.append("<tr>")
            html.append("<td>%s</td>" % summary_count_headers[key])
            html.append("<td>%s</td>" % self[key])
            html.append("<tr>")
        html.append("</table>")

        return ''.join(html)

def add_keys(filename):
    ''' Get auth from our secret keys '''

    global auth
    json_data = open(filename).read()
    keys = json.loads(json_data)
    auth = requests.post('https://dcp.bionimbus.org/user/credentials/cdis/access_token', json=keys)

def query_api(query_txt, variables=None):
    ''' Request results for a specific query '''

    if variables is None:
        query = {'query': query_txt}
    else:
        query = {'query': query_txt, 'variables': variables}

    output = requests.post('https://dcp.bionimbus.org/api/v0/submission/graphql', headers={'Authorization': 'bearer ' + auth.json()['access_token']}, json=query).text
    data = json.loads(output)

    if 'errors' in data:
        print(data)

    return data

def query_summary_counts(project_id):
    ''' Query summary counts for each data type'''

    query_txt = "query Counts ($projectID: [String]) { "
    for count_field in summary_order:
        query_txt += " %s(project_id: $projectID) " % count_field
    query_txt += " }"

    variables = {'projectID': project_id}

    data = query_api(query_txt, variables)

    table = SummaryTable(data['data'])

    return table

def query_summary_field(field, field_node, project_id=None):
    ''' Query summary counts for specific node'''

    if project_id is not None:
        query_txt = """query { %s(first:0, project_id: "%s") {%s}} """ % (field_node, project_id, field)
    else:
        query_txt = """query { %s(first:0) {%s project_id}} """ % (field_node, field)

    data = query_api(query_txt)

    summary = {}
    total = []
    for d in data['data'][field_node]:

        if isinstance(d[field], float):
            d[field] = str(d[field])[:-2]

        if 'project_id' in d:
            summary.setdefault(d['project_id'], {})
            summary[d['project_id']].setdefault(d[field], 0)
            summary[d['project_id']][d[field]] += 1
            if d[field] not in total:
                total.append(d[field])
        else:
            summary.setdefault(d[field], 0)
            summary[d[field]] += 1

    if project_id is not None:
        plot_field_metrics(summary, field)
    else:
        plot_overall_metrics(summary, field, total)

    return summary

def plot_field_metrics(summary_counts, field):
    ''' Plot summary results in a barplot '''

    N = len(summary_counts)

    values = []
    types = []

    for n in sorted(summary_counts, key=summary_counts.get, reverse=True):
        values.append(summary_counts[n])
        types.append(n)

    positions = np.arange(N)
    plt.figure(figsize=(3*N, N))

    plt.bar(positions, values, 0.2, align='center', alpha=0.4, color='#add8e6')
    plt.title('Summary counts by (' + field + ')', fontsize=14)
    plt.ylabel('COUNTS', fontsize=12)
    plt.ylim(0, max(values)+100)
    plt.xlabel(field.upper(), fontsize=12)
    plt.xticks(positions, types, fontsize=12)

    for i, v in enumerate(values):
        plt.text(i-0.1, v+10, str(v), color='red', fontsize=14)

    plt.show()


def plot_overall_metrics(summary_counts, field, totals):
    ''' Visualize summary results across projects in a barplot '''

    results = {}
    projects = {}
    for project in summary_counts:

        results[project] = []
        projects.setdefault(project, 0)

        for value in totals:
            if value in summary_counts[project]:
                results[project].append(summary_counts[project][value])
                projects[project] += summary_counts[project][value]
            else:
                results[project].append(0)

    N = len(totals)
    positions = np.arange(N)
    sorted_projects = sorted(projects, key=projects.get, reverse=True)
    bar_size = 0.2

    plots = []
    plt.figure(figsize=(8, 4))
    left = [0]*N
    for pr in sorted_projects:
        p = plt.barh(positions, results[pr], bar_size, left, align='center', alpha=1)
        plots.append(p[0])
        left = list(map(add, left, results[pr]))

    plt.title('Summary counts by (' + field + ')', fontsize=14)
    plt.xlabel('COUNTS', fontsize=12)
    plt.xlim(0, max(left)+50)
    plt.ylabel(field.upper(), fontsize=12)
    plt.yticks(positions, totals, fontsize=12)
    plt.legend(plots, sorted_projects, fontsize=12)

    plt.show()

def field_distribution(field, field_node, project_id, distrib=None, rate=None, bins=None):
    ''' Plot distribution for one field'''

    count_query = """{ _%s_count(project_id: "%s") }""" % (field_node, project_id)
    counts = query_api(count_query)
    counts = counts['data']['_%s_count' % field_node]
    offset = 0
    chunk = 1000

    data = {}
    while offset <= counts:
        query_txt = """query { %s(project_id: "%s", first:%d, offset:%d, order_by_asc: "submitter_id") {%s}} """ % (field_node, project_id, chunk, offset, field)
        output = query_api(query_txt)
        if not data:
            data = output
        else:
            data['data'][field_node] = data['data'][field_node] + output['data'][field_node]
        offset += chunk

    summary = {}
    total = []
    for d in data['data'][field_node]:

        if isinstance(d[field], float):
            d[field] = str(d[field])[:-2]

        if 'project_id' in d:
            summary.setdefault(d['project_id'], {})
            summary[d['project_id']].setdefault(d[field], 0)
            summary[d['project_id']][d[field]] += 1
            if d[field] not in total:
                total.append(d[field])
        else:
            summary.setdefault(d[field], 0)
            summary[d[field]] += 1

    if len(summary) > 10:

        accumulated = []
        for d in data['data'][field_node]:
            if d[field] is not None:
                accumulated.append(float(d[field]))

        # the histogram of the data
        plt.figure(figsize=(8, 4))
        fig, ax = plt.subplots(1, 1)
        n, positions, patches = ax.hist(accumulated, bins, facecolor='b', alpha=0.75)
        total = len(accumulated)

        plt.xlabel(field)
        plt.ylabel('Counts')
        plt.title('Histogram of ' + field)
        plt.grid(True)

    else:

        N = len(summary)

        values = []
        types = []

        for n in sorted(summary, key=summary.get, reverse=True):
            values.append(summary[n])
            types.append(n)

        total = sum(values)
        positions = np.arange(N)
        fig, ax = plt.subplots(1, 1, figsize=(3*N, N))

        ax.bar(positions, values, 0.2, align='center', alpha=0.4, color='b')

        plt.title('Summary counts by (' + field + ')', fontsize=14)
        plt.ylabel('COUNTS', fontsize=12)
        plt.ylim(0, max(values)+5)
        plt.xlabel(field.upper(), fontsize=12)
        plt.xticks(positions, types, fontsize=12)

    # fit curve
    if distrib == 'exponential':
        fit_curve = expon.pdf(positions, 0, 1.0/rate)*total
        ax.plot(positions, fit_curve, 'r-', lw=2)
    if distrib == 'uniform':
        fit_curve = [total/float(len(positions))] * len(positions)
        ax.plot(positions, fit_curve, 'r-', lw=2)

    plt.show()


def run_statistical_test(values):

    pvalues = {}
    for disease in values:
        if disease != 'Healthy':
            test = ranksums(values['Healthy'], values[disease])
            pvalues[disease] = test.pvalue
        else:
            pvalues[disease] = -1

    return pvalues


def demographic_study(project_id):
    ''' Display basic recruitment and demographics counts '''

    count_query = """{ _demographic_count(project_id: "%s") }""" % (project_id)
    counts = query_api(count_query)
    counts = counts['data']['_demographic_count']
    offset = 0
    chunk = 1000
    #counts = 100

    data = {}
    while offset <= counts:
        query_txt = """query { demographic(project_id: "%s", first:%d, offset:%d, order_by_asc: "submitter_id") {
                                age_range
                                race
                                gender
                             }} """ % (project_id, chunk, offset)
        output = query_api(query_txt)
        if not data:
            data = output
        else:
            data['data']['demographic'] = data['data']['demographic'] + output['data']['demographic']
        offset += chunk

    counts = {}
    races = []
    for d in data['data']['demographic']:
        age = d['age_range']
        race = d['race']
        gender = d['gender']
        counts.setdefault(age, {})
        counts[age].setdefault(gender, {})
        counts[age][gender].setdefault(race, 0)
        counts[age][gender][race] += 1
        if race not in races:
            races.append(race)

    fig = plt.figure(figsize=(11, 8))
    ax1 = fig.add_subplot(111)

    races = sorted(races)
    for age in counts:
        for gender in counts[age]:
            y = []
            label_txt = '%s / %s age' % (gender, age)
            for race in races:
                if race in counts[age][gender]:
                    y.append(counts[age][gender][race])
                else:
                    y.append(0)
            ax1.plot(races, y, label=label_txt)

    ax1.legend(loc=2)
    ax1.set_ylabel("Counts", fontsize=14)
    ax1.set_xlabel("Race", fontsize=14)


def calcium_score_survival(project_id, tag=None, curve='survival', variable='chdatt'):
    ''' Compare survival risk for different CAC ranges'''

    count_query = """{ _case_count(project_id: "%s") }""" % (project_id)
    counts = query_api(count_query)
    counts = counts['data']['_case_count']
    offset = 0
    chunk = 500

    data = {}

    cac_ranges = ['>300', '101-300', '1-100', '0']
    filename = '%s_survival.json' % variable
    if os.path.isfile(filename):
        json_data = open(filename).read()
        values = json.loads(json_data)
    else:
        values = {}
        while offset <= counts:
            itime = datetime.datetime.now()
            query_txt = """{ case(project_id: "%s", first:%d, offset:%d, order_by_asc: "submitter_id"){
                                         submitter_id
                                         medical_histories{%s chda}
                                         cardiac_ct_scans{agatum1c}
                                      }
                            }""" % (project_id, chunk, offset, variable)
            output = query_api(query_txt)
            if not data:
                data = output['data']['case']
            else:
                data = data + output['data']['case']
            offset += chunk

            etime = datetime.datetime.now()
            print("Query (%s) %s" % (offset, str(etime-itime)))

        for c in data:
            if 'cardiac_ct_scans' in c and c['cardiac_ct_scans'] and 'medical_histories' in c and c['medical_histories']:

                cac = c['cardiac_ct_scans'][0]['agatum1c']
                years = c['medical_histories'][0][variable]
                censor = c['medical_histories'][0]['chda']

                if cac == 0:
                    cac = '0'
                elif cac > 0 and cac <= 100:
                    cac = '1-100'
                elif cac > 100 and cac <= 300:
                    cac = '101-300'
                else:
                    cac = '>300'

                if years:
                    values.setdefault('cac', [])
                    values['cac'].append(cac)

                    values.setdefault('years', [])
                    values['years'].append(years)

                    values.setdefault('censors', [])
                    values['censors'].append(censor)

        with open(filename, 'w') as fp:
            json.dump(values, fp)

    if tag is None:
        tag = variable

    # Prepare time for the compared groups
    times = [float(x)/365 for x in values['years']]
    times = np.array(times)
    censors = np.array(values['censors'])
    cac_values = np.array(values['cac'])

    if curve == 'survival':
        # Plot Kaplan-Meier curve
        kmf = KaplanMeierFitter()
        first = 0
        for r in cac_ranges:
            ix = cac_values == r
            if first == 0:
                kmf.fit(times[ix], censors[ix], label=r)
                ax = kmf.plot()
                first = 1
            else:
                kmf.fit(times[ix], censors[ix], label=r)
                kmf.plot(ax=ax)

    elif curve == 'hazard':
        # Plot hazard curve
        naf = NelsonAalenFitter()
        first = 0
        for r in cac_ranges:
            ix = cac_values == r
            if first == 0:
                naf.fit(times[ix], censors[ix], label=r)
                ax = naf.plot()
                first = 1
            else:
                naf.fit(times[ix], censors[ix], label=r)
                naf.plot(ax=ax)

    ax.set_ylabel("%", fontsize=12)
    ax.set_title(tag, fontsize=14)
    ax.set_xlabel("Years to event", fontsize=12)

    return times
