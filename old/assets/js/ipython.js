
ipy_show_inputs=false;

function ipy_toggle_inputs(){
    if (ipy_show_inputs){
     $('div.input').hide();
    }else{
     $('div.input').show();
    }
    ipy_show_inputs = !ipy_show_inputs
}

$( document ).ready(function() {
    $('div.input').hide();
});