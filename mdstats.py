import ipywidgets as ipyw
import pandas as pd
import re
import zipfile
from collections import OrderedDict
from copy import deepcopy
from functools import reduce
from hashlib import md5
from IPython.display import HTML, display
from itables import init_notebook_mode, show
from lxml import etree
from pathlib import Path
from xml.sax import saxutils

# colab uses an old version of pandas
if not hasattr(pd.DataFrame, 'map'):
    pd.DataFrame.map = pd.DataFrame.applymap

ISO_EXTRACT_XPATH = '//gmd:resourceConstraints[gmd:MD_LegalConstraints]'
ISO_MASK_XPATH = '//gco:CharacterString | //@codeList | //*[@gco:nilReason="missing"]'
DCAT_EXTRACT_XPATH = '//dcat:distribution[1]//dct:license | //dcat:distribution[1]//dct:accessRights'

NORMALIZER_PATH = 'normalize.xsl'
TRANSFORMER_PATH = 'split-resource-constraints.xsl'
CONVERTER_PATH = 'iso-19139-to-dcat-ap.xsl'

ISO_NS = {
    'gco': 'http://www.isotc211.org/2005/gco',
    'geonet': 'http://www.fao.org/geonetwork',
    'gmd': 'http://www.isotc211.org/2005/gmd',
    'gml': 'http://www.opengis.net/gml/3.2',
    'gmx': 'http://www.isotc211.org/2005/gmx',
    'xlink': 'http://www.w3.org/1999/xlink',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}
DCAT_NS = {
    'adms': 'http://www.w3.org/ns/adms#',
    'cnt': 'http://www.w3.org/2011/content#',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcat': 'http://www.w3.org/ns/dcat#',
    'dct': 'http://purl.org/dc/terms/',
    'dctype': 'http://purl.org/dc/dcmitype/',
    'dqv': 'http://www.w3.org/ns/dqv#',
    'foaf': 'http://xmlns.com/foaf/0.1/',
    'geodcatap': 'http://data.europa.eu/930/',
    'gsp': 'http://www.opengis.net/ont/geosparql#',
    'locn': 'http://www.w3.org/ns/locn#',
    'org': 'http://www.w3.org/ns/org#',
    'owl': 'http://www.w3.org/2002/07/owl#',
    'prov': 'http://www.w3.org/ns/prov#',
    'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    'schema': 'http://schema.org/',
    'sdmx-attribute': 'http://purl.org/linked-data/sdmx/2009/attribute#',
    'skos': 'http://www.w3.org/2004/02/skos/core#',
    'udata': 'https://github.com/opendatateam/udata',
    'vcard': 'http://www.w3.org/2006/vcard/ns#'
}

HEAD_TAG = '_ROOT'
ERROR_TAG = '_ERROR'

def list_records(path):
    for p in path.iterdir():
        if not p.is_dir():
            continue
        md = p / 'metadata' / 'metadata.xml'
        if not md.exists():
            continue
        yield {'r_id': p.name, 'path': md}

def get_xpath(root, xpath, namespaces):
    if root.tag == ERROR_TAG:
        return root
    r = etree.Element(HEAD_TAG)
    for e in root.xpath(xpath, namespaces=namespaces):
        r.append(deepcopy(e))
    etree.cleanup_namespaces(r, top_nsmap=namespaces)
    return r

# def ns(xpath, namespaces):
#     for k, v in namespaces.items():
#         xpath = re.sub(f'\\b{k}:', f'{{{v}}}', xpath)
#     return xpath

def mask_xpath(root, xpath, namespaces):
    if root.tag == ERROR_TAG:
        return root
    r = deepcopy(root)
    for e in r.xpath(xpath, namespaces=namespaces):
        if etree.iselement(e):
            e.getparent().remove(e)
        else:
            del e.getparent().attrib[e.attrname]
    return r

def maybe_xfunc(xfunc, xpath, namespaces):
    if not xpath or not xpath.strip():
        return None
    return lambda root: xfunc(root, xpath, namespaces)

def maybe_xslt(path):
    if not path or not Path(path).is_file():
        return None
    transform = etree.XSLT(etree.parse(path))
    def _transform(root):
        if root.tag == ERROR_TAG:
            return root
        try:
            return transform(root).getroot()
        except etree.XSLTApplyError as err:
            r = etree.Element(ERROR_TAG)
            r.text = str(err)
            return r
    return _transform

def escape_xml(list_or_string):
    def _transform(s):
        s = saxutils.escape(s)
        s = re.sub('\n', '<br/>', s)
        return s
    if isinstance(list_or_string, list):
        return [_transform(s) for s in list_or_string]
    else:
        return _transform(list_or_string)

def unescape_xml(list_or_string):
    def _transform(s):
        s = re.sub('<br/>', '\n', s)
        s = saxutils.unescape(s)
        return s
    if isinstance(list_or_string, list):
        return [_transform(s) for s in list_or_string]
    else:
        return _transform(list_or_string)

def display_tree(root):
    r = deepcopy(root)
    etree.indent(r)
    s = etree.tostring(r, pretty_print=True, encoding='unicode')
    # remove placeholder root tag => possibly invalid xml from now on
    s = re.sub(f"<{r.tag}[^>]*>\n?", '', s)
    s = re.sub(f"</{r.tag}>$\n?", '', s)
    s = re.sub(f"<{r.tag}/>$\n?", 'NONE', s)
    # de-indent everything since we dropped head tag
    s = re.sub('(\n|^)  ', '\\1', s)
    s = escape_xml(s)
    return s

def to_csv(df, filename='data.csv', dedup=True, unescape=None, listify=None):
    df = df.copy()
    if dedup:
        df = df.drop_duplicates(ignore_index=True)
    if unescape:
        df[unescape] = df[unescape].map(unescape_xml)
    if listify:
        df[listify] = df[listify].map(lambda l: ",".join(l))
    df.to_csv(filename, index=False)

def hash_id(s):
    return md5(s.encode('utf-8'), usedforsecurity=False).hexdigest()

def mdstats_df(records_path,
               iso_extract_xpath, iso_mask_xpath=None, dcat_extract_xpath=None,
               normalizer_path=NORMALIZER_PATH, transformer_path=None, converter_path=None):
    # parser = etree.XMLParser(ns_clean=True, remove_blank_text=True, remove_comments=True)

    iso_extract = maybe_xfunc(get_xpath, iso_extract_xpath, ISO_NS)
    iso_mask = maybe_xfunc(mask_xpath, iso_mask_xpath, ISO_NS)
    dcat_extract = maybe_xfunc(get_xpath, dcat_extract_xpath, DCAT_NS)

    normalize = maybe_xslt(normalizer_path)
    transform = maybe_xslt(transformer_path)
    convert = maybe_xslt(converter_path)

    records = list_records(records_path)
    df = pd.DataFrame.from_records(records)

    df['iso_tree'] = df['path'].map(lambda p: etree.fromstring(p.read_bytes()))  # works with zipfile too
    df['extract'] = df['iso_tree'].map(iso_extract)
    df['pattern'] = df['extract'].map(iso_mask)
    df[['pattern', 'extract']] = df[['pattern', 'extract']].map(normalize).map(display_tree)

    df = (
        df
        # .query("id in ['05f23c86-ad9f-410a-9168-0ffe2879cb74','bdcd66c4-9a2a-47bf-abb3-ed2e144dc8f5','52e0c57d-fd48-4225-917c-6560d7bbd2e6','a7f3ed5d-a511-448b-98a2-de6654c0e839']")
        .groupby(['pattern', 'extract'])
        .agg(
            count=('r_id', 'size'),
            transform=('iso_tree', lambda s: s.iloc[0]),  # only transform 1st in each group
            r_ids=('r_id', lambda s: tuple(s))  # must be hashable
        )
        .reset_index()
    )
    df['total'] = df.groupby('pattern')['count'].transform('sum')

    extra_cols = []
    if transform:
        extra_cols.append('transform')
        df['transform'] = df['transform'].map(transform)
        # delay extract/display so convert can use the full tree
    if convert:
        extra_cols.append('dcat')
        df['dcat'] = df['transform'].map(convert)
        if dcat_extract:
            df['dcat'] = df['dcat'].map(dcat_extract)
        df['dcat'] = df['dcat'].map(display_tree)
    if transform:
        df['transform'] = df['transform'].map(iso_extract).map(display_tree)

    df = df.sort_values(['total', 'count'], ascending=False).reset_index(drop=True)

    # df['id'], _ = pd.factorize(df['pattern'], sort=False)
    df[['p_id', 'e_id']] = df[['pattern', 'extract']].map(hash_id)

    cols = ['p_id', 'e_id', 'total', 'count', 'pattern', 'extract'] + extra_cols + ['r_ids']
    df = df.reindex(columns=cols, fill_value = '')

    print(f"Parsed {df['count'].sum()} records")
    return df

def mdstats_widget_func(records_path, normalizer_path, converter_path):
    def _func(iso_extract_xpath, iso_mask_xpath, dcat_extract_xpath, transformer_path):
        df = mdstats_df(
            records_path=records_path,
            iso_extract_xpath=iso_extract_xpath,
            iso_mask_xpath=iso_mask_xpath,
            dcat_extract_xpath=dcat_extract_xpath,
            normalizer_path=normalizer_path,
            transformer_path=transformer_path,
            converter_path=converter_path
        )

        coldefs = [
            {'targets': 0, 'name': 'p_id', 'visible': False, 'searchPanes': {'header': 'Patterns'}},
            {'targets': 1, 'name': 'e_id', 'visible': False},
            {'targets': 2, 'name': 'total', 'visible': False},
            {'targets': 3, 'name': 'count', 'orderData': [2, 3]}
        ]

        visible = [c for c in ['pattern', 'extract', 'transform', 'dcat'] if c in df.columns]
        width = 95/len(visible)
        for c in visible:
            coldefs.append({'targets': len(coldefs), 'name': c, 'width': f"{width}%",
                            'className': 'dt-left', 'orderable': False})

        coldefs.append({'targets': len(coldefs), 'name': 'r_ids', 'visible': False})

        # show() caught and handled by w.interactive
        show(
            df,
            classes='display',
            column_filters='header',
            columnDefs=coldefs,
            layout={
                'top2':  'searchPanes',
                'topStart': 'info',
                'topEnd': {'buttons': ['copy', 'csv']}
            },
            order=[[2, 'desc'], [3, 'desc']],
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
            # style='table-layout:auto; width:100%;',
            style='width:100%;'
        )
        return df
    return _func

def mdstats_widget(records_path, normalizer_path=NORMALIZER_PATH, converter_path=CONVERTER_PATH):
    # Here instead of list_records because of https://github.com/jupyter-widgets/ipywidgets/issues/3208
    if not records_path.is_dir():
        raise RuntimeError(f"Invalid path: '{records_path}'")

    input_iso_extract = ipyw.Text(value=ISO_EXTRACT_XPATH)
    input_iso_extract.layout.width = '80%'
    input_iso_mask = ipyw.Text(value=ISO_MASK_XPATH)
    input_iso_mask.layout.width = '80%'
    input_dcat_extract = ipyw.Text(value=DCAT_EXTRACT_XPATH)
    input_dcat_extract.layout.width = '80%'
    input_transform = ipyw.Text(value=TRANSFORMER_PATH)
    input_transform.layout.width = '80%'

    w = ipyw.interactive(
        mdstats_widget_func(records_path, normalizer_path, converter_path),
        {'manual': False, 'manual_name': 'Update'},
        iso_extract_xpath=input_iso_extract,
        iso_mask_xpath=input_iso_mask,
        dcat_extract_xpath=input_dcat_extract,
        transformer_path=input_transform
    )

    return w
