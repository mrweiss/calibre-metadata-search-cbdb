#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)

__license__   = 'GPL v3'
__copyright__ = '2013, Ignac Cerda <cerda@centrum.cz>'
__docformat__ = 'restructuredtext cs'

import socket, re, datetime
from collections import OrderedDict
from threading import Thread

from lxml.html import fromstring, tostring

from calibre.ebooks.metadata.book.base import Metadata
from calibre.library.comments import sanitize_comments_html
from calibre.utils.cleantext import clean_ascii_chars

import calibre_plugins.CBDB.config as cfg
import calibre_plugins.CBDB as base

class Worker(Thread): # Get details

    '''
    Get book details from CBDB book page in a separate thread
    '''

    def __init__(self, url, result_queue, browser, log, relevance, plugin, timeout=20):
        Thread.__init__(self)
        self.daemon = True
        self.url = url
        self.result_queue = result_queue
        self.log = log 
        self.timeout = int(timeout)
        self.relevance = relevance
        self.plugin = plugin
        self.browser = browser.clone_browser()
        self.cover_urls = self.CBDB_id = self.isbn = None

    def run(self):
        try:
            self.get_details()
        except:
            self.log.exception('get_details failed for url: %r'%self.url)

    def get_details(self):
        try:
            self.log.info('CBDB book url: %r'%self.url)
            ### offline test
            raw = self.browser.open_novisit(self.url, timeout=self.timeout).read().strip()
            raw = raw.decode('utf-8', errors='replace')
            #open('S:\\d.html', 'wb').write(raw)
            ###raw = open('S:\\d.html', 'rb').read()
                        
        except Exception as e:
            if callable(getattr(e, 'getcode', None)) and \
                    e.getcode() == 404:
                self.log.error('URL malformed: %r'%self.url)
                return
            attr = getattr(e, 'args', [None])
            attr = attr if attr else [None]
            if isinstance(attr[0], socket.timeout):
                msg = 'CBDB timed out. Try again later.'
                self.log.error(msg)
            else:
                msg = 'Failed to make details query: %r'%self.url
                self.log.exception(msg)
            return

        if '<title>404 - ' in raw:
            self.log.error('URL malformed: %r'%self.url)
            return

        try:
            cln = clean_ascii_chars(raw)
            idxs = cln.find('<!DOCTYPE')
            
            if (idxs == -1):
                log.error('Failed to find HTML document')
                return
                        
            root = fromstring(cln[idxs:])
            
        except:
            msg = 'Failed to parse CBDB details page: %r'%self.url
            self.log.exception(msg)
            return

        try:
            # Look at the <title> attribute for page to make sure that we were actually returned
            # a details page for a book. If the user had specified an invalid ISBN, then the results
            # page will just do a textual search.
            title_node = root.xpath('//title')
            if title_node:
                page_title = title_node[0].text_content().strip()
                if page_title is None or page_title.find('search results for') != -1:
                    self.log.error('Failed to see search results in page title: %r'%self.url)
                    return
        except:
            msg = 'Failed to read CBDB page title: %r'%self.url
            self.log.exception(msg)
            return

        errmsg = root.xpath('//*[@id="errorMessage"]')
        if errmsg:
            msg = 'Failed to parse CBDB details page: %r'%self.url
            msg += tostring(errmsg, method='text', encoding=unicode).strip()
            self.log.error(msg)
            return

        self.parse_details(root)

    def parse_details(self, root):
        try:
            CBDB_id = self.parse_CBDB_id(self.url)
        except:
            self.log.exception('Error parsing CBDB id for url: %r'%self.url)
            CBDB_id = None

        try:
            (title, series, series_index) = self.parse_title_series(root)
        except:
            self.log.exception('Error parsing title and series for url: %r'%self.url)
            title = series = series_index = None

        try:
            authors = self.parse_authors(root)
        except:
            self.log.exception('Error parsing authors for url: %r'%self.url)
            authors = []

        if not title or not authors or not CBDB_id:
            self.log.error('Could not find title/authors/CBDB id for %r'%self.url)
            self.log.error('CBDB: %r Title: %r Authors: %r'%(CBDB_id, title, authors))
            return

        mi = Metadata(title, authors)
        if series:
            mi.series = series
            mi.series_index = series_index
        #mi.identifiers['cbdb'] = CBDB_id
        mi.set_identifier('cbdb', CBDB_id)
        #self.log.info(CBDB_id)
        #self.log.info(mi.identifiers.get('cbdb', None))
        self.CBDB_id = CBDB_id        

        try:
            mi.rating = self.parse_rating(root)
        except:
            self.log.exception('Error parsing ratings for url: %r'%self.url)

        # summary
        try:
            mi.comments = self.parse_comments(root)
        except:
            self.log.exception('Error parsing comments for url: %r'%self.url)

        try:
            self.cover_urls = self.parse_covers(root)
        except:
            self.log.exception('Error parsing cover for url: %r'%self.url)
        mi.has_cover = bool(self.cover_urls)
        #self.log.info('covers')
        #self.log.info(self.cover_urls)

        try:
            tags = self.parse_tags(root)
            if tags:
                mi.tags = tags
        except:
            self.log.exception('Error parsing tags for url: %r'%self.url)

        try:
            mi.publisher, mi.pubdate, isbn = self.parse_editions(root)
            if isbn:
                 self.isbn = mi.isbn = isbn
        except:
            self.log.exception('Error parsing publisher and date for url: %r'%self.url)

        mi.source_relevance = self.relevance
        
        mi.language = 'Czech'

        #self.log.info('self.CBDB_id = ' + str(self.CBDB_id ))
        
        if self.CBDB_id:
            if self.isbn:
                self.plugin.cache_isbn_to_identifier(self.isbn, self.CBDB_id)
                
            if self.cover_urls:
                self.plugin.cache_identifier_to_cover_url(self.CBDB_id, self.cover_urls)
                
        self.plugin.clean_downloaded_metadata(mi)

        self.result_queue.put(mi)

    def parse_CBDB_id(self, url):
        #self.log.info(url)
        #self.log.info(url.split('/')[-1])
        return url.split('/')[-1].split('-')[1]

    def parse_title_series(self, root):
        title_node = root.xpath('//div[@class="content"]/div/h1/span')
        if not title_node:
            return (None, None, None)
            
        title_text = title_node[0].text_content().strip()
        if title_text.find('(') == -1:
            return (title_text, None, None)
            
        # Contains a Title and possibly a series. Possible values currently handled:
        # "Some title (Omnibus)"
        # "Some title (#1-3)"
        # "Some title (Series #1)"
        # "Some title (Series (digital) #1)"
        # "Some title (Series #1-5)"
        # "Some title (NotSeries #2008 Jan)"
        # "Some title (Omnibus) (Series #1)"
        # "Some title (Omnibus) (Series (digital) #1)"
        # "Some title (Omnibus) (Series (digital) #1-5)"
        text_split = title_text.rpartition('(')
        title = text_split[0]
        series_info = text_split[2]
        hash_pos = series_info.find('#')
        if hash_pos <= 0:
            # Cannot find the series # in expression or at start like (#1-7)
            # so consider whole thing just as title
            title = title_text
            series_info = ''
        else:
            # Check to make sure we have got all of the series information
            series_info = series_info[:len(series_info)-1] #Strip off trailing ')'
            while series_info.count(')') != series_info.count('('):
                title_split = title.rpartition('(')
                title = title_split[0].strip()
                series_info = title_split[2] + '(' + series_info
        if series_info:
            series_partition = series_info.rpartition('#')
            series_name = series_partition[0].strip()
            if series_name.endswith(','):
                series_name = series_name[:-1]
            series_index = series_partition[2].strip()
            if series_index.find('-'):
                # The series is specified as 1-3, 1-7 etc.
                # In future we may offer config options to decide what to do,
                # such as "Use start number", "Use value xxx" like 0 etc.
                # For now will just take the start number and use that
                series_index = series_index.partition('-')[0].strip()
            try:
                return (title.strip(), series_name, float(series_index))
            except ValueError:
                # We have a series index which isn't really a series index
                title = title_text
        return (title.strip(), None, None)

    def parse_authors(self, root):
        authors = []
        get_all_authors = cfg.plugin_prefs[cfg.STORE_NAME][cfg.KEY_GET_ALL_AUTHORS]
        if get_all_authors:
            author_node = root.xpath('//table[@id="book_info"]/tr/td[@class="v_top"]/a')
            self.log.info(author_node)
            if author_node:
                authors = []
                for author_value in author_node:
                    author = tostring(author_value, method='text', encoding=unicode).strip()
                    # If multiple authors with some as editors can result in a trailing , to remove
                    if author[-1:] == ',':
                        author = author[:len(author)-1]
                    authors.append(author)
                return authors
        else:
            author_node = root.xpath('//table[@id="book_info"]/tr/td[@class="v_top"]/a[@itemprop="author"]')

            if author_node:
                for author_value in author_node:
                    author = author_value.xpath('./strong')[0].text_content().strip()
                    #author_url = author_value.xpath('./@href')[0]
                    authors.append(author)  
                
            return authors

    def parse_rating(self, root):
        rating_node = root.xpath('//div[@itemprop="aggregateRating"]/strong/span[@id="book_rating_text"]')
        if rating_node:
            rating_text = rating_node[0].text_content().strip()
            #self.log.info(rating_text)
            rating_text = re.sub('[^0-9]', '', rating_text)
            rating_value = float(rating_text)
            if rating_value >= 100:
                return rating_value / 100
            #self.log.info(rating_value)
            return rating_value

    def parse_comments(self, root):
        # Look for description in a second span that gets expanded when interactively displayed [@id="display:none"]
        description_node = root.xpath('//div[@id="annotation"]')
        if description_node:
            desc = description_node[0].text_content().strip()
            comments = sanitize_comments_html(desc)
            while comments.find('  ') >= 0:
                comments = comments.replace('  ',' ')
            return comments

    def parse_covers(self, root):
        img_urls = None
        # single cover
        imgcol_node = root.xpath('//table[@id="book_info"]/tr/td[@id="book_covers"]/img[@id="book_img"]')        
        if imgcol_node:
            img_url = imgcol_node[0].xpath('./@src')[0].strip()
            img_url = base.BASE_URL + '/' + img_url
            img_urls = []
            img_urls.append(img_url)
        else:
            # multiple covers
            imgcol_node = root.xpath('//table[@id="book_info"]/tr/td[@id="book_covers"]/div')
            if imgcol_node:
                img_urls = []
                for single_node in imgcol_node:
                    img_url = single_node.xpath('./img[@id="book_img"]/@src')[0].strip()
                    img_url = base.BASE_URL + '/' + img_url
                    img_urls.append(img_url)
                #for        
        return img_urls

    def parse_editions(self, root):
        publisher = None
        pub_date = None
        pub_isbn = None
        publisher_node = root.xpath('//div[@id="releases"]/table/tr')
        if publisher_node:
            # <div id="releases">
            #  <table>
            #   <tr>
            #   <td><strong>Nakladatelství&nbsp;(rok)</strong></td>
            #   <td><strong>ISBN</strong></td>
            #   <td><strong>Pocet&nbsp;stran</strong></td>
            #   <td><strong>Poznámka</strong></td>
            #   </tr>
            #   <tr>
            #     <td>
            #       Rozmluvy
            #       (2009)
            #     </td>
            #     <td>
            #       978-80-85336-67-2
            #     </td>
            #     <td>
            #       120
            #     </td>
            #     <td class="releases_note">
            #     </td>
            #   </tr>
            #   <tr>
            #     <td>
            #       Ceskoslovenský spisovatel
            #       (1970)
            #     </td>
            #     <td>
            #     </td>
            #     <td>
            #     </td>
            #     <td class="releases_note">
            #     </td>
            #   </tr>
            #   <tr>
            #     <td>
            #       Štorch-Marien
            #       (1924)
            #     </td>
            #     <td>
            #     </td>
            #     <td>
            #     </td>
            #     <td class="releases_note">
            #     </td>
            #   </tr>
            #   </table>
            #   <span class="show_covers" onClick="hide_releases();">Skrýt vydání</span><br /><br />
            # </div>
            #self.log.info(publisher_node.__len__())
            
            cnt = publisher_node.__len__()
            i = 1
            while i < cnt:
                pub_edition = publisher_node[i].xpath('./td')
                
                if pub_edition:
                    publisher_text = pub_edition[0].text_content().strip()
                    
                    # publisher & publication date
                    pub = publisher_text.split('(')
                    if pub[1]:
                        psher = pub[0].strip()
                        pdate = pub[1].strip()
                        pdate = pdate[:-1]
                        #self.log.info(psher)
                        #self.log.info(pdate)
                        
                        if (publisher == None):
                            publisher = ''
                        else:
                            publisher += ', '
                                
                        publisher += psher + ' | ' + pdate
                    
                    # isbn
                    pub = pub_edition[1].text_content().strip()
                    if pub:
                        #self.log.info(pub)
                        if (pub_isbn == None):
                            pub_isbn = pub
                        
                        publisher += ' | ' + pub                          
                                            
                i += 1
            #wend
            
            if (cnt == 2):
                pub = publisher.split('|')
                publisher = pub[0].strip()
                pubdate_text = pub[1].strip()
                
                if pubdate_text:
                    pub_date = self._convert_date_text(pubdate_text)
            
            #self.log.info(publisher)
            #self.log.info(pub_date)
            #self.log.info(pub_isbn)
        return (publisher, pub_date, pub_isbn)

    def parse_tags(self, root):
        # CBDB does not have "tags", but it does have Genres (wrapper around popular shelves)
        # We will use those as tags (with a bit of massaging)
        genres_node = root.xpath('//div[@class="stacked"]/div/div/div[contains(@class, "bigBoxContent")]/div/div')
        if genres_node:
            genre_tags = list()
            for genre_node in genres_node:
                sub_genre_nodes = genre_node.xpath('a')
                genre_tags_list = [sgn.text_content().strip() for sgn in sub_genre_nodes]
                if genre_tags_list:
                    genre_tags.append(' > '.join(genre_tags_list))
            calibre_tags = self._convert_genres_to_calibre_tags(genre_tags)
            if len(calibre_tags) > 0:
                return calibre_tags

    def _convert_genres_to_calibre_tags(self, genre_tags):
        # for each tag, add if we have a dictionary lookup
        calibre_tag_lookup = cfg.plugin_prefs[cfg.STORE_NAME][cfg.KEY_GENRE_MAPPINGS]
        calibre_tag_map = dict((k.lower(),v) for (k,v) in calibre_tag_lookup.iteritems())
        tags_to_add = list()
        for genre_tag in genre_tags:
            tags = calibre_tag_map.get(genre_tag.lower(), None)
            if tags:
                for tag in tags:
                    if tag not in tags_to_add:
                        tags_to_add.append(tag)
        return list(tags_to_add)

    def _convert_date_text(self, date_text):
        # Note that the date text could be "2003", "December 2003" or "December 10th 2003"
        year = int(date_text[-4:])
        month = 1
        day = 1
        if len(date_text) > 4:
            text_parts = date_text[:len(date_text)-5].partition(' ')
            month_name = text_parts[0]
            # Need to convert the month name into a numeric value
            # For now I am "assuming" the CBDB website only displays in English
            # If it doesn't will just fallback to assuming January
            month_dict = {"January":1, "February":2, "March":3, "April":4, "May":5, "June":6,
                "July":7, "August":8, "September":9, "October":10, "November":11, "December":12}
            month = month_dict.get(month_name, 1)
            if len(text_parts[2]) > 0:
                day = int(re.match('([0-9]+)', text_parts[2]).groups(0)[0])
        from calibre.utils.date import utc_tz
        return datetime.datetime(year, month, day, tzinfo=utc_tz)
