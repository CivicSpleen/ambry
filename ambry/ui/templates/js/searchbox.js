// Author: Ryan Heath
// http://rpheath.com
// From: https://raw.githubusercontent.com/rpheath/searchbox/master/searchbox.js
// Github: https://github.com/rpheath/searchbox

(function ($) {

    $.searchbox = {}

    $.extend(true, $.searchbox, {

        settings: {
            url: '/search',
            param: 'query',
            dom_id: '#results',
            delay: 100,
            loading_css: '#loading',
            term_func: null
        },


        loading: function () {
            $($.searchbox.settings.loading_css).show()
        },

        resetTimer: function (timer) {
            if (timer) clearTimeout(timer)
        },

        idle: function () {
            $($.searchbox.settings.loading_css).hide()
        },

        process: function (terms) {

            var path = $.searchbox.settings.url.split('?'), base = path[0], params = path[1]

            if (typeof terms == 'string'){
                var query_string = [$.searchbox.settings.param, '=', terms].join('')
            } else {
                var query_string = terms.join('&')
            }

            var query = query_string

            if (params) query_string = [params.replace('&amp;', '&'), query].join('&')

            var url = [base, '?', query_string].join('')

            $.get(url, function (data) {
                $($.searchbox.settings.dom_id).html(data)
            })
        },

        start: function () {
            $(document).trigger('before.searchbox')
            $.searchbox.loading()
        },

        stop: function () {
            $.searchbox.idle()
            $(document).trigger('after.searchbox')
        }
    })

    $.fn.searchbox = function (config) {
        $.searchbox.settings = $.extend(true, $.searchbox.settings, config || {})

        $(document).trigger('init.searchbox')
        $.searchbox.idle()

        return this.each(function () {
            var $input = $(this)

            $input.focus()
                .ajaxStart(function () {
                    $.searchbox.start()
                })
                .ajaxStop(function () {
                    $.searchbox.stop()
                })
                .keyup(function () {
                    var val;

                    if ($input.prop('tagName') == 'INPUT') {
                        val = $input.val()
                        var test_val = val
                     } else {
                        var query_parts = []
                        $input.find('input, select').each(function( index ){
                            query_parts.push( [$(this).attr('name'), '=', encodeURIComponent($(this).val())].join(''))
                        })
                        val = query_parts
                        var test_val = String(val)
                    }

                    console.log(test_val)

                    if (test_val != this.previousValue) {
                        $.searchbox.resetTimer(this.timer)

                        this.timer = setTimeout(function () {
                            $.searchbox.process(val)
                        }, $.searchbox.settings.delay)

                        this.previousValue = test_val
                    }
                })
        })
    }
})(jQuery);