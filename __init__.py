#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)

__license__ = 'GPL v3'
__copyright__ = '2013, Ignac Cerda <cerda@centrum.cz>'
__docformat__ = 'restructuredtext cs'

import time
import unicodedata
import string

from urllib import quote
from Queue import Queue, Empty

from lxml.html import fromstring, tostring

from calibre import as_unicode
from calibre.ebooks.metadata import check_isbn
from calibre.ebooks.metadata.sources.base import Source
from calibre.utils.icu import lower
from calibre.utils.cleantext import clean_ascii_chars

from calibre import ipython

BASE_URL = 'http://www.cbdb.cz'
BASE_BOOK_URL = '%s/kniha-%s'
MAX_EDITIONS = 5


class CBDB(Source):

    name = 'CBDB'
    description = _('Downloads metadata and covers from CBDB')
    author = 'Ignac Cerda'
    version = (0, 0, 5)
    minimum_calibre_version = (0, 8, 0)

    capabilities = frozenset(['identify', 'cover'])
    touched_fields = frozenset(['title', 'authors', 'identifier:cbdb',
                                'identifier:isbn', 'rating', 'comments', 'publisher', 'pubdate',
                                'tags', 'series'])
    has_html_comments = True
    supports_gzip_transfer_encoding = True
    can_get_multiple_covers = True

    def config_widget(self):
        '''
        Overriding the default configuration screen for our own custom configuration
        '''
        from calibre_plugins.CBDB.config import ConfigWidget
        return ConfigWidget(self)

    def get_book_url(self, identifiers, log=None):
        CBDB_id = identifiers.get('cbdb', None)
        # if log:
        # log.info('CBDB_id')
        # log.info(CBDB_id)
        if CBDB_id:
            return ('cbdb', CBDB_id,
                    BASE_BOOK_URL % (BASE_URL, CBDB_id))

    def get_cached_cover_urls(self, identifiers, log=None):
        urls = None
        CBDB_id = identifiers.get('cbdb', None)
        # if log:
        # log.info('CBDB_id')
        # log.info(CBDB_id)
        if CBDB_id is None:
            isbn = identifiers.get('isbn', None)
            if isbn is not None:
                CBDB_id = self.cached_isbn_to_identifier(isbn)
        if CBDB_id is not None:
            urls = self.cached_identifier_to_cover_url(CBDB_id)
        return urls

    def identify(self, log, result_queue, abort, title=None, authors=None, identifiers={}, timeout=30, nested=False):
        matches = []

        ipython(locals())

        CBDB_id = identifiers.get('cbdb', None)
        isbn = check_isbn(identifiers.get('isbn', None))
        br = self.browser

        if CBDB_id:
            matches.append(BASE_BOOK_URL % (BASE_URL, CBDB_id))
        else:
            query = self.create_query(
                log, title=title, authors=authors, identifiers=identifiers)
            if query is None:
                log.error('Insufficient metadata to construct query')
                return
            try:
                log.info('Querying: %s' % query)
                response = br.open_novisit(query, timeout=timeout)
                if isbn:
                    # Check whether we got redirected to a book page for ISBN searches.
                    # If we did, will use the url.
                    # If we didn't then treat it as no matches on CBDB
                    location = response.geturl()
                    if '/book/show/' in location:
                        log.info('ISBN match location: %r' % location)
                        matches.append(location)
            except IOError as e:
                err = 'Connection problem. Check your Internet connection'
                log.warning(err)
                return as_unicode(e)

            except Exception as e:
                err = 'Failed to make identify query: %r' % query
                # testing w/o inet
                log.exception(err)
                return as_unicode(e)

            # For ISBN based searches we have already done everything we need to
            # So anything from this point below is for title/author based searches.
            if not isbn:
                try:
                    raw = response.read().strip()
                    #open('E:\\t.html', 'wb').write(raw)
                    ###raw = open('S:\\t.html', 'rb').read()
                    raw = raw.decode('utf-8', errors='replace')
                    if not raw:
                        log.error(
                            'Failed to get raw result for query: %r' % query)
                        return

                    cln = clean_ascii_chars(raw)
                    idxs = cln.find('<!DOCTYPE')
                    if (idxs == -1):
                        log.error('Failed to find HTML document')
                        return

                    vld = cln[idxs:]
                    # log.info(vld)

                    idxs = vld.find("<head>")
                    if (idxs == -1):
                        log.error('Failed to find HEAD element')
                        return

                    # <!DOCTYPE .. <head>
                    hdr = vld[:idxs]

                    idxs = vld.find('<h2>Nalezeno')
                    if (idxs == -1):
                        log.error('Incorrect document structure 1')
                        return

                    idxe = vld.find('</h2>', idxs)
                    if (idxe == -1):
                        log.error('Incorrect document structure 2')
                        return

                    arr = vld[idxs:idxe].split(':')
                    if (arr.__len__() != 2):
                        log.error('Incorrect document structure 3')
                        return

                    cnt = int(arr[1])
                    # a publication found
                    if (cnt != 0):
                        hdr += '<HEAD/>' + '<BODY>' + \
                            '<H3>' + str(cnt) + '</H3>'

                        idxs = vld.find('<table', idxe)
                        if (idxs == -1):
                            log.error('Incorrect document structure 11')
                            return

                        idxe = vld.find('</table>', idxs)
                        if (idxe == -1):
                            log.error('Incorrect document structure 12')
                            return

                        hdr += vld[idxs:(idxe + 8)] + '</BODY>' + '</HTML>'

                        # rebuild HTML to contain just relevant data
                        # first line ~ result count
                        # table ~ results
                        vld = hdr
                    else:
                        # nothing found, so send an empty HTML
                        vld = '<HTML/>'

                    # log.info('vld')
                    # log.info(vld)
                    root = fromstring(vld)

                except:
                    msg = 'Failed to parse CBDB page for query: %r' % query
                    log.exception(msg)
                    return msg

                # Now grab values from the search results, provided the
                # title and authors appear to be for the same book
                self._parse_search_results(
                    log, title, authors, root, matches, timeout)

        if abort.is_set():
            return

        if (matches.__len__() == 0):
            if nested:
                return

            log.info('No matches found, trying to strip accents')

            if (not self.identify(log, result_queue, abort, title=self.strip_accents(title), authors=self.strip_accents(authors), timeout=30, nested=True)):
                log.info('No matches found, trying to strip numbers')

                if (not self.identify(log, result_queue, abort, title=self.strip_accents(title.rstrip(string.digits)), authors=self.strip_accents(authors), timeout=30, nested=True)):
                    log.error('No matches found with query: %r' % query)

            return

        #log.info('Lets process matches ...')
        from calibre_plugins.CBDB.worker import Worker
        workers = [Worker(url, result_queue, br, log, i, self)
                   for i, url in enumerate(matches)]

        for w in workers:
            w.start()
            # Don't send all requests at the same time
            time.sleep(0.1)

        while not abort.is_set():
            a_worker_is_alive = False
            for w in workers:
                w.join(0.2)
                if abort.is_set():
                    break
                if w.is_alive():
                    a_worker_is_alive = True
            if not a_worker_is_alive:
                break

        return None

    # disable isbn merging
    def merge_identify_results(self, result_map, log):
        return result_map

    def create_query(self, log, title=None, authors=None, identifiers={}):
        isbn = check_isbn(identifiers.get('isbn', None))
        q = ''
        if isbn is not None:
            q = 'isbn=' + isbn
        elif title:
            tokens = []
            title_tokens = list(self.get_title_tokens(
                title, strip_joiners=False, strip_subtitle=True))
            tokens += title_tokens
            '''
            author_tokens = self.get_author_tokens(authors,
                    only_first_author=True)
            tokens += author_tokens
            '''
            tokens = [quote(t.encode('utf-8') if isinstance(t,
                                                            unicode) else t) for t in tokens]
            q = 'type=book&name=' + '+'.join(tokens)
        elif authors:
            tokens = []
            author_tokens = self.get_author_tokens(authors,
                                                   only_first_author=True)
            tokens += author_tokens
            tokens = [quote(t.encode('utf-8') if isinstance(t,
                                                            unicode) else t) for t in tokens]
            q = 'type=author&name=' + '+'.join(tokens)

        if not q:
            return None
        if isinstance(q, unicode):
            q = q.encode('utf-8')
        return BASE_URL + '/vyhledavani.php?ok=VYHLEDAT&' + q

    def strip_accents(self, inp):
        if isinstance(inp, list):
            li = []
            for s in inp:
                li.append(self.strip_accents(s))

            return li
        return ''.join((c for c in unicodedata.normalize('NFD', inp) if unicodedata.category(c) != 'Mn'))

    def _parse_search_results(self, log, orig_title, orig_authors, root, matches, timeout):
        header = root.xpath('//h3')
        if not header:
            return

        if (header.__len__() != 1):
            log.error('Incorrect data structure - hlen - ')
            log.info(header.__len__())
            return

        def ismatch(title, authors, title_tokens, author_tokens):
            authors = lower(' '.join(authors))
            title = lower(title)

            match = not title_tokens
            for t in title_tokens:
                if lower(t) in title:
                    match = True
                    break

            amatch = not author_tokens
            for a in author_tokens:
                if lower(a) in authors:
                    amatch = True
                    break

            if not author_tokens:
                amatch = True

            return match and amatch

        title_tokens = list(self.get_title_tokens(orig_title))
        author_tokens = list(self.get_author_tokens(orig_authors))
        ntitle_tokens = list(self.get_title_tokens(
            self.strip_accents(orig_title)))
        nauthor_tokens = list(self.get_author_tokens(
            self.strip_accents(orig_authors)))
        # log.info(orig_title)
        # log.info(orig_authors)
        # log.info(self.strip_accents(orig_title))
        # log.info(self.strip_accents(orig_authors))

        cnt = int(header[0].text)
        # log.info(cnt)
        i = 0
        while i < cnt:
            i += 1
            # log.info(i)
            xresult = root.xpath('//table/tr[' + str(i) + ']/td')
            if not xresult:
                return

            # log.info(xresult.__len__())
            # log.info(xresult[1].xpath('./a')[0].text_content())
            # log.info(xresult[3].xpath('./a')[0].text_content())
            # log.info(xresult[0].xpath('./img/@src')[0])
            title = xresult[1].xpath(
                './a')[0].text_content().strip().decode('utf-8', errors='replace')
            authors = xresult[3].xpath(
                './a')[0].text_content().strip().decode('utf-8', errors='replace').split(',')
            ntitle = self.strip_accents(title)
            nauthors = self.strip_accents(authors)
            #rank = xresult[0].xpath('./img/@src')[0][13]

            if not ismatch(title, authors, title_tokens, author_tokens):
                if not ismatch(ntitle, nauthors, ntitle_tokens, nauthor_tokens):
                    log.error('Rejecting as not close enough match: %s %s' %
                              (title, authors))
                    continue

            xresult_url_node = xresult[1].xpath('./a/@href')
            if xresult_url_node:
                result_url = BASE_URL + '/' + xresult_url_node[0]
                log.info('RURL ' + result_url)
                matches.append(result_url)

    def _parse_editions_for_book(self, log, editions_url, matches, timeout, title_tokens):

        def ismatch(title):
            title = lower(title)
            match = not title_tokens
            for t in title_tokens:
                if lower(t) in title:
                    match = True
                    break
            return match

        br = self.browser
        try:
            raw = br.open_novisit(editions_url, timeout=timeout).read().strip()
        except Exception as e:
            err = 'Failed identify editions query: %r' % editions_url
            log.exception(err)
            return as_unicode(e)
        try:
            raw = raw.decode('utf-8', errors='replace')
            if not raw:
                log.error('Failed to get raw result for query: %r' %
                          editions_url)
                return
            #open('E:\\s.html', 'wb').write(raw)
            root = fromstring(clean_ascii_chars(raw))
        except:
            msg = 'Failed to parse CBDB page for query: %r' % editions_url
            log.exception(msg)
            return msg

        first_non_valid = None
        for div_link in root.xpath('//div[@class="editionData"]/div[1]/a[@class="bookTitle"]'):
            title = tostring(div_link, 'text').strip().lower()
            if title:
                # Verify it is not an audio edition
                valid_title = True
                for exclusion in ['(audio cd)', '(compact disc)', '(audio cassette)']:
                    if exclusion in title:
                        log.info('Skipping audio edition: %s' % title)
                        valid_title = False
                        if first_non_valid is None:
                            first_non_valid = BASE_URL + div_link.get('href')
                        break
                if valid_title:
                    # Verify it is not a foreign language edition
                    if not ismatch(title):
                        log.info('Skipping alternate title:', title)
                        continue
                    matches.append(BASE_URL + div_link.get('href'))
                    if len(matches) >= CBDB.MAX_EDITIONS:
                        return
        if len(matches) == 0 and first_non_valid:
            # We have found only audio editions. In which case return the first match
            # rather than tell the user there are no matches.
            log.info('Choosing the first audio edition as no others found.')
            matches.append(first_non_valid)

    def download_cover(self, log, result_queue, abort, title=None, authors=None, get_best_cover=None, identifiers={}, timeout=30):
        cached_urls = self.get_cached_cover_urls(identifiers, log)
        # log.info('dc')
        # log.info(cached_url)
        if cached_urls is None:
            log.info('No cached cover found, running identify')
            rq = Queue()
            self.identify(log, rq, abort, title=title,
                          authors=authors, identifiers=identifiers)
            if abort.is_set():
                return
            results = []
            while True:
                try:
                    results.append(rq.get_nowait())
                except Empty:
                    break
            results.sort(key=self.identify_results_keygen(
                title=title, authors=authors, identifiers=identifiers))
            for mi in results:
                cached_urls = self.get_cached_cover_urls(mi.identifiers, log)
                if cached_urls is not None:
                    break

        if cached_urls is None:
            log.info('No cover found')
            return

        if abort.is_set():
            return

        br = self.browser
        for cached_url in cached_urls:
            log('Downloading covers from:', cached_url)
            try:
                cdata = br.open_novisit(cached_url, timeout=timeout).read()
                result_queue.put((self, cdata))
            except:
                log.exception('Failed to download cover from:', cached_url)


if __name__ == '__main__':  # tests
    # To run these test use:
    # calibre-debug -e __init__.py
    from calibre.ebooks.metadata.sources.test import (test_identify_plugin,
                                                      title_test, authors_test, series_test)
    test_identify_plugin(CBDB.name,
                         [
                             (  # A book throwing an index error
                                 {'title': 'The Girl Hunters',
                                     'authors': ['Mickey Spillane']},
                                 [title_test('The Girl Hunters', exact=True),
                                  authors_test(['Mickey Spillane']),
                                     series_test('Mike Hammer', 7.0)]
                             ),

                             (  # A book with no ISBN specified
                                 {'title': "Harry Potter and the Sorcerer's Stone",
                                     'authors': ['J.K. Rowling']},
                                 [title_test("Harry Potter and the Sorcerer's Stone", exact=True),
                                  authors_test(['J. K. Rowling']),
                                     series_test('Harry Potter', 1.0)]

                             ),

                             (  # A book with an ISBN
                                 {'identifiers': {'isbn': '9780385340588'},
                                  'title': '61 Hours', 'authors': ['Lee Child']},
                                 [title_test('61 Hours', exact=True),
                                  authors_test(['Lee Child']),
                                     series_test('Jack Reacher', 14.0)]

                             ),

                             (  # A book with a CBDB id
                                 {'identifiers': {'cbdb': '6977769'},
                                  'title': '61 Hours', 'authors': ['Lee Child']},
                                 [title_test('61 Hours', exact=True),
                                  authors_test(['Lee Child']),
                                     series_test('Jack Reacher', 14.0)]

                             ),

                         ])
