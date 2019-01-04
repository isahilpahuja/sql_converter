import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChangeScalaToSqlComponent } from './change-scala-to-sql.component';

describe('ChangeScalaToSqlComponent', () => {
  let component: ChangeScalaToSqlComponent;
  let fixture: ComponentFixture<ChangeScalaToSqlComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChangeScalaToSqlComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChangeScalaToSqlComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
