import { Component, OnInit } from '@angular/core';
import { catchError, map, tap } from 'rxjs/operators';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { ProgressSpinnerModule } from 'primeng/progressspinner';

@Component({
  selector: 'app-change-scala-to-sql',
  templateUrl: './change-scala-to-sql.component.html',
  styleUrls: ['./change-scala-to-sql.component.scss']
})
export class ChangeScalaToSqlComponent implements OnInit {

  db = '';
  query: any;
  convertedQuery: any;
  convertedQueryException: any;
  isShowSpinner: boolean;

  constructor(private http: HttpClient) { }

  ngOnInit() {
    this.db = 'MySql';
  }

  submit(conversionType: String) {
    const queryWithoutLeadingSPace = this.query ? this.query.replace(/^\s+/gm, '') : '';
    const body = {
      databaseName: this.db,
      sqlQuery: queryWithoutLeadingSPace,
      conversionType: conversionType
    };
    this.isShowSpinner = true;
    if (conversionType === 'Scala') {
      this.http.post('http://10.17.200.92:9000/sqlToDataFrameConverter', body, { responseType: 'text' })
        .pipe(map((response: any) => response)).subscribe(
          (data: any) => {
            const convertedData = data;
            this.convertedQuery = convertedData.substring(0, convertedData.indexOf('#'));
            this.convertedQueryException = convertedData.substring(convertedData.indexOf('#') + 1);
            this.isShowSpinner = false;
          },
          err => {
            alert('Oops!! Something Went Wrong!!');
            this.convertedQuery = '';
            this.convertedQueryException = '';
            this.isShowSpinner = false;
          }
        );
    } else {
      this.http.post('http://10.17.200.92:9000/testSqlPost', body, { responseType: 'text' })
        .pipe(map((response: any) => response)).subscribe(
          (data: any) => {
            const convertedData = data;
            this.convertedQuery = convertedData.substring(0, convertedData.indexOf('#'));
            this.convertedQueryException = convertedData.substring(convertedData.indexOf('#') + 1);
            this.isShowSpinner = false;
          },
          err => {
            alert('Oops!! Something Went Wrong!!');
            this.convertedQuery = '';
            this.convertedQueryException = '';
            this.isShowSpinner = false;
          }
        );
    }

  }

  download() {
    const element = document.createElement('a');
    if (!this.convertedQuery) {
      alert('No data Found!');
      return false;
    }
    element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(this.convertedQuery));
    element.setAttribute('download', 'Scala_Query');
    element.style.display = 'none';
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
  }

  reset() {
    this.db = 'MySql';
    this.query = '';
    this.convertedQuery = '';
    this.convertedQueryException = '';
    this.isShowSpinner = false;
  }
}
