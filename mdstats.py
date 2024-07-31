import ipywidgets as ipyw
import pandas as pd
import re
import zipfile
from collections import OrderedDict
from copy import deepcopy
from functools import reduce
from IPython.display import HTML, display
from itables import init_notebook_mode, show
from lxml import etree
from pathlib import Path
from xml.sax import saxutils

# colab uses an old version of pandas
if not hasattr(pd.DataFrame, 'map'):
    pd.DataFrame.map = pd.DataFrame.applymap

EXTRACT_XPATH = '//gmd:resourceConstraints[gmd:MD_LegalConstraints]'
MASK_XPATHS = '\n'.join(['//gco:CharacterString', '//@codeList', '//*[@gco:nilReason="missing"]'])

HEADTAG = 'root'

NS = {
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gco': 'http://www.isotc211.org/2005/gco',
    'gml': 'http://www.opengis.net/gml/3.2',
    'gmx': 'http://www.isotc211.org/2005/gmx',
    'xlink': 'http://www.w3.org/1999/xlink',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'geonet': 'http://www.fao.org/geonetwork'
}

def list_records(path):
    for p in path.iterdir():
        if not p.is_dir():
            continue
        md = p / 'metadata' / 'metadata.xml'
        if not md.exists():
            continue
        yield {'id': p.name, 'path': md}

def get_xpath(tree, xfunc):
    root = etree.Element(HEADTAG)
    elems = xfunc(tree)
    for e in elems:
        root.append(e)
    etree.cleanup_namespaces(root, top_nsmap=NS)
    return root

# def ns(xpath):
#     for k, v in NS.items():
#         xpath = re.sub(f'\\b{k}:', f'{{{v}}}', xpath)
#     return xpath

def strip_xpath(tree, *xpath):
    t = deepcopy(tree)
    for x in xpath:
        for e in t.xpath(x, namespaces=NS):
            if etree.iselement(e):
                e.getparent().remove(e)
            else:
                del e.getparent().attrib[e.attrname]
    return t

def escape_xml(list_or_string):
    if isinstance(list_or_string, list):
        return [saxutils.escape(x) for x in list_or_string]
    else:
        return saxutils.escape(list_or_string)

def display_tree(tree):
    t = deepcopy(tree)
    etree.indent(t)
    s = etree.tostring(t, pretty_print=True, encoding='unicode')
    # remove head tag => possibly invalid xml from now on
    s = re.sub(f"^<{HEADTAG} [^>]*>\n", '', s)
    s = re.sub(f"</{HEADTAG}>$\n", '', s)
    # de-indent everything since we dropped head tag
    s = re.sub('^  ', '', s)
    s = escape_xml(s)  # FIXME: can we avoid this?
    s = re.sub('\n', '<br/>', s)
    return s

def mdstats_func(records_path, normalizer_path):
    normalize = etree.XSLT(etree.parse(normalizer_path))

    def _func(extract_xpath, mask_xpaths):
        # parser = etree.XMLParser(ns_clean=True, remove_blank_text=True, remove_comments=True)

        extract_xfunc = etree.XPath(extract_xpath, namespaces=NS, smart_strings=False)
        mask_xpaths = [l.strip() for l in mask_xpaths.splitlines() if l.strip()]

        records = list_records(records_path)
        df = pd.DataFrame.from_records(records)

        df['tree'] = df['path'].map(lambda p: etree.fromstring(p.read_bytes()))  # works with zipfile too
        df['extract'] = df['tree'].map(lambda t: get_xpath(t, extract_xfunc))
        df['pattern'] = df['extract'].map(lambda t: strip_xpath(t, *mask_xpaths))
        df[['pattern', 'extract']] = df[['pattern', 'extract']].map(normalize).map(display_tree)
        df = df.drop(columns=['path', 'tree'])

        df = (
            df
            # .query("id in ['05f23c86-ad9f-410a-9168-0ffe2879cb74','bdcd66c4-9a2a-47bf-abb3-ed2e144dc8f5','52e0c57d-fd48-4225-917c-6560d7bbd2e6','a7f3ed5d-a511-448b-98a2-de6654c0e839']")
            .groupby(['pattern', 'extract'])
            .agg(
                count=('id', 'size')
                #ids=('id', lambda s: list(s)),
            )
            .reset_index()
        )
        df['total'] = df.groupby('pattern')['count'].transform('sum')
        df['gid'], _ = pd.factorize(-df['total'], sort=True)
        df = df.reindex(columns=['gid', 'pattern', 'extract', 'total', 'count'])

        # show() is handled by w.interactive
        show(df,
             classes='display',
             column_filters='header',
             columnDefs=[
                 {'targets': 0, 'name': 'gid', 'visible': False, 'searchPanes': {'header': 'Patterns'}},
                 {'targets': 1, 'name': 'pattern', 'width': '45%'},
                 {'targets': 2, 'name': 'extract', 'width': '50%'},
                 {'targets': 3, 'name': 'total', 'visible': False},
                 {'targets': 4, 'nane': 'count', 'orderData': [3, 4]},
                 {'targets': [1, 2], 'className': 'dt-left', 'orderable': False},
             ],
             layout={
                 'top2':  'searchPanes',
                 'topStart': 'info',
                 'topEnd': {'buttons': ['copy', 'csv']}
             },
             order=[[3, 'desc'], [4, 'desc']],
             paging=True,
             rowGroup={'dataSrc': 0, 'className': 'row-group'},
             scrollCollapse=True,
             # scrollY='400px',  # FIXME: breaks table width
             searchPanes={
                 'clear': True,
                 'collapse': False,
                 'columns': [0],
                 'controls': False,
                 'initCollapsed': True,
                 'layout': 'columns-1',
                 'orderable': False,  # buggy
             },
             select=True,
             #style='table-layout:auto; width:100%;',
             style='width:100%;'
        )
        print(f"Parsed {df['count'].sum()} records")
        return df

    return _func


def mdstats_widget(records_path, normalizer_path='normalize.xsl'):
    # Here instead of list_records because of https://github.com/jupyter-widgets/ipywidgets/issues/3208
    if not records_path.is_dir():
        raise RuntimeError(f"Invalid path: '{records_path}'")

    input_extract = ipyw.Text(value=EXTRACT_XPATH)
    input_extract.layout.width = '80%'
    # FIXME: input_mask fixed font
    input_mask = ipyw.Textarea(value=MASK_XPATHS)
    input_mask.layout.width = '80%'

    w = ipyw.interactive(
        mdstats_func(records_path, normalizer_path),
        {'manual': False, 'manual_name': 'Update'},
        extract_xpath=input_extract,
        mask_xpaths=input_mask)

    return w
