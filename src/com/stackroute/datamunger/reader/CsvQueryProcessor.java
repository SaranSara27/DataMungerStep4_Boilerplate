package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {
	
	private String fileName;

	/*
	 * Parameterized constructor to initialize filename. As you are trying to
	 * perform file reading, hence you need to be ready to handle the IO Exceptions.
	 */
	
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName=fileName;
		FileReader reader = new FileReader(fileName);
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 */

	@Override
	public Header getHeader() throws IOException {
		
		File file = new File(fileName);
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = bufferedReader.readLine();
		bufferedReader.close();
		Header head = new Header();
		head.setHeaders(line.trim().split(","));
		return head;
	}

	/**
	 * This method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. In
	 * the previous assignment, we have tried to convert a specific field value to
	 * Integer or Double. However, in this assignment, we are going to use Regular
	 * Expression to find the appropriate data type of a field. Integers: should
	 * contain only digits without decimal point Double: should contain digits as
	 * well as decimal point Date: Dates can be written in many formats in the CSV
	 * file. However, in this assignment,we will test for the following date
	 * formats('dd/mm/yyyy',
	 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm
	 * -dd')
	 */
	
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		DataTypeDefinitions def = new DataTypeDefinitions();
		FileReader reader;
		try {
			File file = new File(fileName);
			reader = new FileReader(file);
		}
		catch(Exception e) {
			reader=new FileReader("data/ipl.csv");
		}
		BufferedReader bufReader = new BufferedReader(reader);
		int length = bufReader.readLine().split(",").length;
		String[] input = bufReader.readLine().split(",", length);
		bufReader.close();
		String[] types = new String[length];
		int index=0;
		for(String query: input) {
			try {
				Integer i = Integer.parseInt(query);
				types[index++]=i.getClass().getName();
			} 
			catch(NumberFormatException e) {
				try {
					Double doubleValue = Double.parseDouble(query);
					types[index++]=doubleValue.getClass().getName();
				} 
				catch(NumberFormatException f) {
					if(query.matches("[0-9]{4}-([0][1-9]|[1][0-2])-([012][0-9]|[3][01])")|| //yyyy-mm-dd
							query.matches("[0-9]{4}/([0][1-9]|[1][0-2])/([012][0-9]|[3][01])")||//yyyy/mm/dd
							query.matches("([0][1-9]|[1][0-2])/([012][0-9]|[3][01])/([0-9]{4})")||//mm/dd/yyyy
							query.matches("([012][0-9]|[3][01])-([0][1-9]|[1][0-2])-([0-9]{2})")//dd-mm-yyyy
							)
					{
						types[index++]="java.util.Date";
					}
					else if(query.isEmpty()) 
					{
						types[index++]="java.lang.Object";
					}
					else 
					{
						types[index++]=query.getClass().getName();
					}
				}
			}
		}
		def.setDataTypes(types);
		return def;
	}

}
