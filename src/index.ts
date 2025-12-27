import { createDb, IDb } from "./db";

const DB_TYPES = {
  EMPLOYEE: "employee",
  CITY: "city",
  DIVISION: "division",
  POSITION: "position",
  EMPLOYEE_WITH_CITY_VIEW: "employeeWithCity_view",
  EMPLOYEE_WITH_POSITION_VIEW: "employeeWithPosition_view",
} as const;

interface DbCity {
  uuid: string;
  name: string;
}

interface DbDivision {
  uuid: string;
  name: string;
  cityUuid: string;
}

interface DbPosition {
  uuid: string;
  name: string;
}

interface DbEmployee {
  uuid: string;
  firstName: string;
  lastName: string;
  divisionUuid: string;
  cityUuid: string;
  positionUuid: string;
}

interface DbEmployeeWithCity {
  uuid: string;
  firstName: string;
  city: string;
}

interface DbEmployeeWithPosition {
  uuid: string;
  firstName: string;
  position: string;
  division: string;
}

interface DbDocument {
  type: string;
  [key: string]: any;
}

// Runtime type guards
function isDbCity(data: unknown): data is DbCity {
  return (
    typeof data === "object" &&
    data !== null &&
    "uuid" in data &&
    "name" in data &&
    typeof (data as any).uuid === "string" &&
    typeof (data as any).name === "string"
  );
}

function isDbDivision(data: unknown): data is DbDivision {
  return (
    typeof data === "object" &&
    data !== null &&
    "uuid" in data &&
    "name" in data &&
    "cityUuid" in data
  );
}

function isDbPosition(data: unknown): data is DbPosition {
  return (
    typeof data === "object" &&
    data !== null &&
    "uuid" in data &&
    "name" in data
  );
}

function isDbEmployee(data: unknown): data is DbEmployee {
  return (
    typeof data === "object" &&
    data !== null &&
    "uuid" in data &&
    "firstName" in data &&
    "divisionUuid" in data &&
    "cityUuid" in data &&
    "positionUuid" in data
  );
}

export const citySource: DbCity[] = [
  { uuid: "3ba648aa-4498-43da-b29f-b83f37a25429", name: "Алматы" },
  { uuid: "32d82d73-3eac-4e5a-9921-fcd2e1447c76", name: "Астана" },
];

export const divisionSource: DbDivision[] = [
  {
    uuid: "97cf9556-2882-4c4a-b7b5-37cf53347447",
    name: "Департамент информационных технологий",
    cityUuid: "3ba648aa-4498-43da-b29f-b83f37a25429",
  },
  {
    uuid: "3e80754a-3681-4e5c-8d6d-b84d09a7a3c4",
    name: "Дирекция",
    cityUuid: "3ba648aa-4498-43da-b29f-b83f37a25429",
  },
];

export const positionSource: DbPosition[] = [
  {
    uuid: "3e80754a-3681-4e5c-8d6d-b84d09a7a3c4",
    name: "Руководитель службы поддержки",
  },
  { uuid: "cc811dfb-7f73-4c18-969f-c8408fd92263", name: "Разработчик" },
];

export const employeeSource: DbEmployee[] = [
  {
    uuid: "65f5c1d4-fb87-4da2-b0bd-a22343605396",
    firstName: "Name 1",
    lastName: "Name 2",
    divisionUuid: "3e80754a-3681-4e5c-8d6d-b84d09a7a3c4",
    cityUuid: "3ba648aa-4498-43da-b29f-b83f37a25429",
    positionUuid: "cc811dfb-7f73-4c18-969f-c8408fd92263",
  },
  {
    uuid: "59e23b74-8645-46d6-9751-5fe594dd89e6",
    firstName: "Name 1",
    lastName: "Name 2",
    divisionUuid: "3e80754a-3681-4e5c-8d6d-b84d09a7a3c4",
    cityUuid: "3ba648aa-4498-43da-b29f-b83f37a25429",
    positionUuid: "cc811dfb-7f73-4c18-969f-c8408fd92263",
  },
];


export interface IHRApp {
  employeeWithCityList: () => Promise<{ firstName: string; city: string }[]>;
  employeeWithPositionList: () => Promise<{
    firstName: string;
    position: string;
    division: string;
  }[]>;
  update: (args: {
    entity: "employee" | "city" | "position" | "division";
    data: object;
  }) => Promise<void>;
}

class HRApp implements IHRApp {
  private db: IDb;
  private cityCache: Map<string, DbCity> = new Map();
  private divisionCache: Map<string, DbDivision> = new Map();
  private positionCache: Map<string, DbPosition> = new Map();

  constructor(db: IDb) {
    this.db = db;
  }

  async initialize(): Promise<void> {
    try {
      await this.loadReferenceData();
      await this.buildMaterializedViews();
    } catch (error) {
      throw new Error(`Initialization failed: ${error}`);
    }
  }

  private async loadReferenceData(): Promise<void> {
    try {
      const cities = await this.db.query({ type: DB_TYPES.CITY, where: {} });
      if (!cities.items || cities.items.length === 0) {
        console.warn("No cities found in database");
      }
      
      for (const item of cities.items) {
        if (isDbCity(item.data)) {
          this.cityCache.set(item.data.uuid, item.data);
        }
      }

      const divisions = await this.db.query({ type: DB_TYPES.DIVISION, where: {} });
      for (const item of divisions.items) {
        if (isDbDivision(item.data)) {
          this.divisionCache.set(item.data.uuid, item.data);
        }
      }

      const positions = await this.db.query({ type: DB_TYPES.POSITION, where: {} });
      for (const item of positions.items) {
        if (isDbPosition(item.data)) {
          this.positionCache.set(item.data.uuid, item.data);
        }
      }
    } catch (error) {
      throw new Error(`Failed to load reference data: ${error}`);
    }
  }

  private async buildMaterializedViews(): Promise<void> {
    try {
      const employees = await this.db.query({ type: DB_TYPES.EMPLOYEE, where: {} });
      
      if (!employees.items || employees.items.length === 0) {
        return;
      }

      // Batch операции для лучшей производительности
      const cityViewPromises = employees.items
        .filter(empRecord => isDbEmployee(empRecord.data))
        .map(async (empRecord) => {
          const emp = empRecord.data as DbEmployee;
          const city = this.cityCache.get(emp.cityUuid);
          
          if (!city) {
            console.warn(`City not found for employee ${emp.uuid}`);
            return;
          }

          const viewData: DbDocument = {
            type: DB_TYPES.EMPLOYEE_WITH_CITY_VIEW,
            uuid: emp.uuid,
            firstName: emp.firstName,
            city: city.name,
          };

          return this.db.post({ record: { data: viewData } });
        });

      const positionViewPromises = employees.items
        .filter(empRecord => isDbEmployee(empRecord.data))
        .map(async (empRecord) => {
          const emp = empRecord.data as DbEmployee;
          const position = this.positionCache.get(emp.positionUuid);
          const division = this.divisionCache.get(emp.divisionUuid);
          
          if (!position || !division) {
            console.warn(`Position or division not found for employee ${emp.uuid}`);
            return;
          }

          const viewData: DbDocument = {
            type: DB_TYPES.EMPLOYEE_WITH_POSITION_VIEW,
            uuid: emp.uuid,
            firstName: emp.firstName,
            position: position.name,
            division: division.name,
          };

          return this.db.post({ record: { data: viewData } });
        });

      await Promise.all([...cityViewPromises, ...positionViewPromises]);
    } catch (error) {
      throw new Error(`Failed to build materialized views: ${error}`);
    }
  }

  async employeeWithCityList(): Promise<{ firstName: string; city: string }[]> {
    try {
      const result = await this.db.query({
        type: DB_TYPES.EMPLOYEE_WITH_CITY_VIEW,
        where: {},
      });

      if (!result.items) {
        return [];
      }

      return result.items
        .filter((item) => {
          const data = item.data as any;
          return data && data.firstName && data.city;
        })
        .map((item) => {
          const data = item.data as DbEmployeeWithCity;
          return {
            firstName: data.firstName,
            city: data.city,
          };
        });
    } catch (error) {
      throw new Error(`Failed to fetch employee with city list: ${error}`);
    }
  }

  async employeeWithPositionList(): Promise<{
    firstName: string;
    position: string;
    division: string;
  }[]> {
    try {
      const result = await this.db.query({
        type: DB_TYPES.EMPLOYEE_WITH_POSITION_VIEW,
        where: {},
      });

      if (!result.items) {
        return [];
      }

      return result.items
        .filter((item) => {
          const data = item.data as any;
          return data && data.firstName && data.position && data.division;
        })
        .map((item) => {
          const data = item.data as DbEmployeeWithPosition;
          return {
            firstName: data.firstName,
            position: data.position,
            division: data.division,
          };
        });
    } catch (error) {
      throw new Error(`Failed to fetch employee with position list: ${error}`);
    }
  }

  async update(): Promise<void> {
    // не требуется по условию задания
  }
}

export const createHRApp = async (): Promise<IHRApp> => {
  const db = createDb();
  const app = new HRApp(db);
  await app.initialize();
  return app;
};
