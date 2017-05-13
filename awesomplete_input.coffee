# awesomplete_input.coffee
"""
Nice autocomplete from this thread: https://github.com/bokeh/bokeh/issues/5596
Project URL: https://leaverou.github.io/awesomplete/
"""

import {TextInput, TextInputView} from "models/widgets/text_input"
import * as p from "core/properties"


export class AwesompleteInputView extends TextInputView

  initialize: (options) ->
    @awesomplete = null
    super(options)
    # already called in super
    # @listenTo(@model, 'change', @render)
    # @render()

  render: () ->
    super()
    #if @awesomplete == null
      # fixme, init-ing over here because render() is called in super() ..
    $input = @$el.find("input")
    $input.addClass("dropdown-input")
    @awesomplete = new Awesomplete($input[0])
    @awesomplete.list = @model.completions
    @awesomplete.minChars = @model.min_chars
    @awesomplete.maxItems = @model.max_items
    @awesomplete.autoFirst = @model.auto_first
    return @

export class AwesompleteInput extends TextInput
  type: "AwesompleteInput"
  default_view: AwesompleteInputView

  # defaults are from awesomplete
  @define {
      completions: [ p.Array, [] ]
      min_chars: [ p.Number, 1 ]
      max_items: [ p.Number, 10 ]
      auto_first: [ p.Bool, false ]
    }