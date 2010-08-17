## Licensed under the Apache License, Version 2.0 (the "License"); you may not
## use this file except in compliance with the License. You may obtain a copy of
## the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
## License for the specific language governing permissions and limitations under
## the License.

cherlylibdir = $(localerlanglibdir)/cherly-$(version)
cherlyprivdir = $(cherlylibdir)/priv
cherlyprivlibdir = $(cherlylibdir)/priv/lib

EI_DIR = @ERL_LIB_DIR_erl_interface@

CHERLY_SRCS = \
	cherly.c \
        cherly.h \
	cherly_drv.c \
	common.h \
        double_link.c \
        double_link.h \
        lru.c \
        lru.h

cherlyprivlib_LTLIBRARIES = cherly_drv.la
cherly_drv_la_SOURCES = $(CHERLY_SRCS)
cherly_drv_la_LDFLAGS = -module -avoid-version $(JUDY_LDFLAGS) -L"${EI_DIR}/lib"
cherly_drv_la_CFLAGS = $(JUDY_CFLAGS) -I"${EI_DIR}/include"
cherly_drv_la_LIBADD = $(JUDY_LDFLAGS) -lei

install-data-hook:
	if test -f "$(DESTDIR)$(cherlyprivlibdir)"; then \
            rm -f "$(DESTDIR)$(cherlyprivlibdir)/couch_drv.so"; \
            cd "$(DESTDIR)$(cherlyprivlibdir)" && \
                $(LN_S) couch_icu_driver couch_icu_driver.so; \
        fi

uninstall-local:
	if test -f "$(DESTDIR)$(cherlyprivlibdir)"; then \
	    rm -f "$(DESTDIR)$(cherlyprivlibdir)/cherly_drv.so"; \
	fi
