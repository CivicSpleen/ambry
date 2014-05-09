<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<html xmlns:py="http://genshi.edgewall.org/" py:strip="" >
	<h2>Schema for ${table.name}</h2>
	<table>
		<tr>
			<th>Column</th>
			<th>Type</th>
			<th>Size</th>
			<th>Description</th>
		</tr>
		<tr py:for="c in table.columns">
        	<td>${c.name}</td>
        	<td>${c.datatype}</td>
        	<td>${c.size}</td>
        	<td>${c.description}</td>
      	</tr>
	</table>
</html>